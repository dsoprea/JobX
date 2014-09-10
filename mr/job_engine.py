"""Process messages off the queue, including mapping, combining, and reducing.
"""

import sys
import logging
import functools
import types
import traceback
import json
import time
import pprint
import itertools

import gevent.pool

import etcd.exceptions

import mr.config
import mr.config.queue
import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.handler
import mr.models.kv.invocation
import mr.models.kv.trees.invocations
import mr.models.kv.queues.dataset
import mr.queue.queue_manager
import mr.workflow_manager
import mr.shared_types
import mr.constants
import mr.handlers.scope

_logger = logging.getLogger(__name__)

# TODO(dustin): We can maintain a nice little cache if the whole cluster pushes 
#               updates to it.

# TODO(dustin): We still have to deal with KV changes not yet having propagated 
#               by the time a queued message is picked-up by a worker.

# TODO(dustin): We might push changes into both *etcd* and *memcache*. Since 
#               there might be only one instance of memcache (and entirely in 
#               memory) vs many instances of *etcd* (and entirely on disk), it 
#               might provide us immediate concurrency while not completely 
#               sacrificing durability (if the process going down, there should 
#               still be a high degree of synchronization).


class _QueuePusher(object):
    def __init__(self):
        self.__q = mr.queue.queue_manager.get_queue()

    def queue_map_step_from_parameters(self, message_parameters):
# TODO(dustin): We might increment a count of total steps processed on the 
#               request.

        replacements = {
            'workflow_name': message_parameters.workflow.workflow_name,
        }

        topic = mr.config.queue.TOPIC_NAME_MAP_TEMPLATE % replacements

        self.__q.producer.push_one(
            topic, 
            mr.constants.D_MAP, 
            message_parameters)

    def queue_initial_map_step_from_parameters(self, message_parameters):
        return self.queue_map_step_from_parameters(message_parameters)

    def queue_reduce_step_from_parameters(self, message_parameters, 
                                          parent_invocation):
# TODO(dustin): This still might need to be reflowed to fit changes.
        """We're reflecting (switch directions from mapping to reduction). The 
        current step is an action step (no mappings were done). The next 
        invocation will successively take the invocation-IDs of one parent to 
        the next.
        """

        reduce_step = mr.models.kv.step.get(
                        message_parameters.workflow, 
                        parent_invocation.step_name)

        _logger.debug("Queueing reduce of step [%s] for parent invocation: [%s]", 
                      reduce_step.step_name, parent_invocation.invocation_id)

        workflow = message_parameters.workflow

        reduce_invocation = mr.models.kv.invocation.Invocation(
                                invocation_id=None,
                                workflow_name=\
                                    workflow.workflow_name,
                                parent_invocation_id=\
                                    parent_invocation.invocation_id,
                                step_name=reduce_step.step_name,
                                direction=mr.constants.D_REDUCE)

        reduce_invocation.save()

        # Create a tracking tree for this new invocation.

        it = mr.models.kv.trees.invocations.InvocationsTree(
                workflow, 
                reduce_invocation)

        it.create()

        # Add an invocation to bind this invocation to our true parent (the 
        # last actual invocation, not the one we're being given).

        parent_tree = mr.models.kv.trees.invocations.InvocationsTree(
                        workflow, 
                        message_parameters.invocation)

        parent_tree.add_entity(reduce_invocation)

        # Queue the reduction.

        reduce_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            workflow=workflow,
            invocation=reduce_invocation,
            request=message_parameters.request,
            job=message_parameters.job,
            step=reduce_step,
            handler=reduce_step.reduce_handler_name)

        assert reduce_parameters.handler is not None

        _logger.debug("Reduction [%s] will be performed over step: [%s].", 
                      reduce_parameters.invocation.invocation_id, 
                      reduce_step.step_name)

        replacements = {
            'workflow_name': workflow.workflow_name,
        }

        topic = mr.config.queue.TOPIC_NAME_REDUCE_TEMPLATE % replacements

        self.__q.producer.push_one(
            topic, 
            mr.constants.D_REDUCE, 
            reduce_parameters)

_pusher = None
def _get_pusher():
    global _pusher

    if _pusher is None:
        _pusher = _QueuePusher()

    return _pusher


class _StepProcessor(object):
    """Receives queued items to be processed. We are running in our own gthread 
    by the time we're called.
    """

    def __queue_map_step(self, next_step, kv_tuple, original_parameters):
        request = original_parameters.request
        workflow = original_parameters.workflow
        job = original_parameters.job
        step = original_parameters.step
        invocation = original_parameters.invocation

        assert invocation.invocation_id is not None

        next_handler = mr.models.kv.handler.get(
                        workflow, 
                        next_step.map_handler_name)

        # The next invocation will have this [mapping] step as a parent.
        map_invocation = mr.models.kv.invocation.Invocation(
                                invocation_id=None,
                                workflow_name=workflow.workflow_name,
                                parent_invocation_id=invocation.invocation_id,
                                step_name=next_step.step_name,
                                direction=mr.constants.D_MAP)

        map_invocation.save()

        # Store the arguments for the new invocation.

        dq = mr.models.kv.queues.dataset.DatasetQueue(
                workflow, 
                map_invocation, 
                mr.models.kv.queues.dataset.DT_ARGUMENTS)

        data = {
            'p': kv_tuple,
        }

        dq.add(data)

        # Create a tracking tree for this new invocation.

        it = mr.models.kv.trees.invocations.InvocationsTree(
                workflow, 
                map_invocation)

        it.create()

        # Add an association to bind this invocation to our parent's.

        parent_tree = mr.models.kv.trees.invocations.InvocationsTree(
                        workflow, 
                        invocation)

        parent_tree.add_entity(map_invocation)

        # Queue the mapping.

        mapped_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            workflow=workflow,
            invocation=map_invocation,
            request=request,
            job=job,
            step=next_step,
            handler=next_handler)

        pusher = _get_pusher()
        pusher.queue_map_step_from_parameters(mapped_parameters)

    def __call_handler(self, workflow, handler_name, arguments):
        wm = mr.workflow_manager.get_wm()
        managed_workflow = wm.get(workflow.workflow_name)
        handlers = managed_workflow.handlers

        _logger.debug("Calling handler [%s] under workflow [%s].", 
                      handler_name, workflow.workflow_name)

        return handlers.run_handler(handler_name, arguments)

    def __default_combiner(self, map_result_gen):
        """The default combiner: group by key."""

        if mr.config.IS_DEBUG is True:
            map_result_gen = list(map_result_gen)
            _logger.debug("Combining (default):\n%s", 
                          pprint.pformat(map_result_gen))

        # itertools.groupby() requires it to be sorted, first.
        sorted_result_gen = (p 
                             for p 
                             in sorted(
                                    map_result_gen, 
                                    key=lambda x: x[0]))

        if mr.config.IS_DEBUG is True:
            sorted_result_gen = list(sorted_result_gen)
            _logger.debug("Pre-combine sort:\n%s",
                          pprint.pformat(sorted_result_gen))

        grouped_result_gen = itertools.groupby(
                                sorted_result_gen, 
                                lambda x: x[0])

        def make_distilled_result_gen():
            for k, value_list in grouped_result_gen:
                yield (k, (v for (_, v) in value_list))

        distilled_result_gen = make_distilled_result_gen()

        if mr.config.IS_DEBUG is True:
            distilled_result_gen = [(k, list(value_list)) 
                                    for (k, value_list) 
                                    in distilled_result_gen]

            _logger.debug("Combiner result:\n%s", 
                          pprint.pformat(distilled_result_gen))

        return distilled_result_gen

    def __apply_combiner(self, workflow, current_step, map_result_gen):
        combine_handler_name = current_step.combine_handler_name

        if combine_handler_name is not None:
            combine_handler = functools.partial(
                                self.__call_handler, 
                                workflow, 
                                combine_handler_name)

            return combine_handler(map_result_gen)
        else:
            return self.__default_combiner(map_result_gen)

    def __map_to_downstream(self, mapped_step_name, handler_name, 
                            mapped_steps_gen, workflow, invocation, 
                            message_parameters):
        """A mapping step has completed and has mapped into one or more 
        downstream steps. Queue the downstream steps to be handled and tracked.
        """

        assert invocation.mapped_count is None
        assert invocation.mapped_waiting is None

        mapped_step = mr.models.kv.step.get(workflow, mapped_step_name)

        # This has to be an integer just in case one of the downstream steps 
        # completes before we finish our accounting, here.

        invocation.mapped_waiting = 0
        invocation.save()

        i = 0
        for (k, v) in mapped_steps_gen:
            _logger.debug("Queueing mapping (%d) from invocation [%s].",
                          i, invocation.invocation_id)

            self.__queue_map_step(
                    mapped_step, 
                    (k, v), 
                    message_parameters)

            i += 1

        step_count = i

        # Now, update the number of mapped steps into the invocation. 
        #
        # Because we either decrement or add maximum positive value, and the 
        # value of mapped_waiting will never be glimpsed before decrementing 
        # it, there won't be any chance of a completed step seeing the 
        # mappd_waiting value equal zero more than once (which is our trigger 
        # for a reduction), which will be the very last manipulation it 
        # counters.

        _logger.debug("Invocation [%s] has mapped (%d) steps.", 
                      invocation.invocation_id, step_count)

# TODO(dustin): We might need to check for whether a reduction is necessary 
#               here. By the time we get here, we could've potentially finished 
#               all steps, which nothing else checking for (0) waiting-steps.

        invocation = self.__add_mapped_steps(
                        workflow, 
                        invocation, 
                        step_count)

        _logger.debug("Invocation [%s] has had its counts updated: MC=(%d) "
                      "MW=(%d)", 
                      invocation.invocation_id, invocation.mapped_count, 
                      invocation.mapped_waiting)

    def __map_collect_result(self, handler_name, handler_result_gen, workflow, 
                             invocation, message_parameters):
        """The mapper returned a generator of key-value pairs (rather than 
        mapping to another downstream step). This is essentially an invocation
        "leaf" that will end any branching activity, and either contribute one
        child of a step that mapped or complete the request.
        """

        # Wrap the result generator in a combiner generator. By the time we get 
        # the data, it'll already be combined.
        #
        # This ensures that we have the opportunity to flatten the data between 
        # descending map operations. Note that the default combiner groups by 
        # key, but does not flatten the value (concatenation, summing, etc..). 
        # So, descending maps will be grouped, grouped a second time, grouped a 
        # third time, etc.. It's probably almost always desired to provide a 
        # combiner if we have more than one dimension of steps.
        map_result_gen = self.__apply_combiner(
                            workflow, 
                            message_parameters.step, 
                            handler_result_gen)

        _logger.debug("Writing result-set for invocation: [%s]", 
                      invocation.invocation_id)

        dq = mr.models.kv.queues.dataset.DatasetQueue(
                workflow, 
                invocation,
                mr.models.kv.queues.dataset.DT_POST_COMBINE)

        if mr.config.IS_DEBUG is True:
            map_result_gen = [(k, list(v)) for (k, v) in map_result_gen]
            _logger.debug("Result to be stored:\n%s", 
                          pprint.pformat(map_result_gen))

        i = 0
        for (k, value_list) in map_result_gen:
            data = { 
                'k': k,
                'vl': list(value_list),
            }

            dq.add(data)
            i += 1

        _logger.debug("Result-set of size (%d) written for invocation [%s]. "
                      "Queueing reduction.", i, invocation.invocation_id)

        # We're here because a map operation rendered a result (and did not map 
        # further downstream). It's tempting to want to reduce here, but we'd 
        # end up compromising the whole concept of map-reduce, and we might 
        # potential be processing a high-cost mapping *and* a high-cost 
        # reduction within the same invocation.

        pusher = _get_pusher()

        # Do a reduction with this invocation as the parent (it will access our 
        # results).
        pusher.queue_reduce_step_from_parameters(
            message_parameters, 
            invocation)

    def handle_map(self, message_parameters):
        """Handle one dequeued map job."""

        request = message_parameters.request
        step = message_parameters.step
        invocation = message_parameters.invocation
        workflow = message_parameters.workflow
        
        _logger.debug("Processing MAP: [%s]", invocation.invocation_id)

        try:
            # Call the handler.

            dq = mr.models.kv.queues.dataset.DatasetQueue(
                    workflow, 
                    invocation, 
                    mr.models.kv.queues.dataset.DT_ARGUMENTS)
# TODO(dustin): Arguments should be pairs of keys and value-lists from the very 
#               moment the request is received, since downstream mappings 
#               receive them that way.
            # Enumerate the 'p' member of every record.
            arguments = (d['p'] for d in dq.list_data())

            if mr.config.IS_DEBUG is True:
                arguments = list(arguments)
                _logger.debug("Sending arguments to mapper:\n%s", 
                              pprint.pformat(arguments))

            wrapped_arguments = {
                'arguments': arguments,
            }

            handler_result_gen = self.__call_handler(
                                    workflow, 
                                    step.map_handler_name, 
                                    wrapped_arguments)

            path_type = next(handler_result_gen)

            _logger.debug("Mapper [%s] path-type: [%s]", 
                          invocation.invocation_id, 
                          path_type.__class__.__name__)

            assert issubclass(
                    path_type.__class__, 
                    mr.handlers.scope.MrConfigure) is True

            # Manage downstream steps that were mapped to (the handler was a 
            # generator).

            if issubclass(
                   path_type.__class__, 
                   mr.handlers.scope.MrConfigureToMap) is True:

                self.__map_to_downstream(
                    path_type.next_step_name,
                    step.map_handler_name, 
                    handler_result_gen,
                    workflow, 
                    invocation, 
                    message_parameters)
            elif issubclass(
                    path_type.__class__, 
                    mr.handlers.scope.MrConfigureToReturn) is True:

                self.__map_collect_result(
                    step.map_handler_name,
                    handler_result_gen,
                    workflow, 
                    invocation,
                    message_parameters)
        except:
# TODO(dustin): Mark the request as failed, and have whatever is blocking on a 
#               result track down the error message/traceback.
# TODO(dustin): We might have to remove the chain of invocations, on error.
            invocation.error = traceback.format_exc()
            invocation.save()

            raise

    def __decrement_invocation(self, workflow, invocation):
        def get_cb():
            obj = mr.models.kv.invocation.get(
                    workflow, 
                    invocation.invocation_id)

            return obj

        def set_cb(obj):
            obj.mapped_waiting -= 1

        obj = mr.models.kv.invocation.Invocation.atomic_update(get_cb, set_cb)
        return obj

    def __add_mapped_steps(self, workflow, invocation, step_count):
        def get_cb():
            return mr.models.kv.invocation.get(
                    workflow, 
                    invocation.invocation_id)

        def set_cb(obj):
            obj.mapped_count = step_count
            obj.mapped_waiting += step_count

        return mr.models.kv.invocation.Invocation.atomic_update(get_cb, set_cb)

    def handle_reduce(self, message_parameters):
        """Corresponds to steps received with a type of mr.constants.D_REDUCE.

        As we work our way down from the request/job/original-step to 
        successive mappings, we link them by way of the parent_invocation_id.
        When we're working our way up through reduction, the 
        parent_invocation_id of each reduction invocation points to the 
        invocation record that we're reducing. We'll then continue to queue 
        successive invocation for the parents of parents, until we make it all
        of the way to the original step (which will have no parent).
        """

        step = message_parameters.step
        reduce_invocation = message_parameters.invocation
        workflow = message_parameters.workflow
        request = message_parameters.request

        assert step.reduce_handler_name is not None

        try:
            # The parent of the current invocation is the invocation that had all 
            # of the mappings to be reduced.

            map_invocation = mr.models.kv.invocation.get(
                                workflow, 
                                reduce_invocation.parent_invocation_id)

            if map_invocation.mapped_waiting is None:
                _logger.debug("Processing REDUCE [%s] -of- original MAP "
                              "invocation [%s] that rendered a DATASET.",
                              reduce_invocation.invocation_id, 
                              map_invocation.invocation_id)

                return self.__handle_mapped_dataset_reduce(
                        message_parameters,
                        step, 
                        map_invocation,
                        workflow, 
                        request)
            else:
                _logger.debug("Processing REDUCE [%s] -of- original MAP "
                              "invocation [%s] that rendered DOWNSTREAM "
                              "MAPPINGS.",
                              reduce_invocation.invocation_id, 
                              map_invocation.invocation_id)

                return self.__handle_mapped_mapping_reduce(
                        message_parameters,
                        step, 
                        map_invocation,
                        workflow, 
                        request)
        except:
# TODO(dustin): Mark the request as failed, and have whatever is blocking on a 
#               result track down the error message/traceback.
# TODO(dustin): We might have to remove the chain of invocations, on error.
            reduce_invocation.error = traceback.format_exc()
            reduce_invocation.save()

            raise

    def __handle_mapped_mapping_reduce(self, message_parameters, step, 
                                       map_invocation, workflow, request):
        """Reduce over a mapping invocation that rendered a dataset."""

        # Call the handler with a generator of all of the results to be 
        # reduced.

        def get_results_gen():
            """Enumerate all (key, value_list) from all results of all 
            invocations mapped from this invocation.

            Note that, no matter how good the combiner is, if one step maps 
            into downstream steps than there could very well have duplicate 
            keys (which is a relatively normal circumstance, but entirely 
            unavoidable of multidimensional-mappings).
            """

            parent_tree = mr.models.kv.trees.invocations.InvocationsTree(
                            workflow, 
                            map_invocation)

            for mapped_invocation in parent_tree.list_entities():
                dq = mr.models.kv.queues.dataset.DatasetQueue(
                        workflow, 
                        mapped_invocation,
                        mr.models.kv.queues.dataset.DT_POST_COMBINE)

                for data in dq.list_data():
                    yield data

        results_gen = get_results_gen()

        if mr.config.IS_DEBUG is True:
            results_gen = list(results_gen)

            _logger.debug("(%d) results will be reduced by step [%s] for "
                          "original invocation [%s].",
                          len(results_gen), step.reduce_handler_name, 
                          map_invocation.invocation_id)
            
            print('')
            for (i, data) in enumerate(results_gen):
                print("Result (%d)\nKey: %s\n Value List: %s" % 
                      (i, data['k'], data['vl']))

            print('')

        results_gen = ((data['k'], data['vl']) for data in results_gen)

        handler_arguments = {
            'results': results_gen,
        }

        reduce_result_gen = self.__call_handler(
                                workflow,
                                step.reduce_handler_name, 
                                handler_arguments)
# TODO(dustin): We need to translate our list of resultant key-value pairs back 
#               into key and value-list pairs, for consistency with upstream 
#               reductions.
        if mr.config.IS_DEBUG is True:
            reduce_result_gen = list(reduce_result_gen)            
            _logger.debug("Handler [%s] reduction [%s] result:\n%s", 
                          step.reduce_handler_name, 
                          map_invocation.invocation_id,
                          pprint.pformat(reduce_result_gen))

        if map_invocation.parent_invocation_id is not None:
            decrement_invocation = mr.models.kv.invocation.get(
                                    workflow, 
                                    map_invocation.parent_invocation_id)
        else:
            decrement_invocation = None

        self.__store_reduction_result(
            message_parameters,
            reduce_result_gen, 
            map_invocation,
            decrement_invocation=decrement_invocation)

    def __handle_mapped_dataset_reduce(self, message_parameters, step, 
                                       map_invocation, workflow, request):
        """Reduce over a mapping invocation that mapped to downstream steps."""

        # Call the handler with a generator of all of the results to be 
        # reduced.

        dq = mr.models.kv.queues.dataset.DatasetQueue(
                workflow, 
                map_invocation,
                mr.models.kv.queues.dataset.DT_POST_COMBINE)

        results_gen = dq.list_data()

        if mr.config.IS_DEBUG is True:
            results_gen = list(results_gen)

            _logger.debug("(%d) results will be reduced by step [%s] for "
                          "original invocation [%s].",
                          len(results_gen), step.reduce_handler_name, 
                          map_invocation.invocation_id)
            
            print('')
            for (i, data) in enumerate(results_gen):
                print("Result (%d)\nKey: %s\n Value List: %s" % 
                      (i, data['k'], data['vl']))

            print('')

        results_gen = ((data['k'], data['vl']) for data in results_gen)

        handler_arguments = {
            'results': results_gen,
        }

        reduce_result_gen = self.__call_handler(
                                workflow,
                                step.reduce_handler_name, 
                                handler_arguments)
# TODO(dustin): We need to translate our list of resultant key-value pairs back 
#               into key and value-list pairs, for consistency with upstream 
#               reductions.
        if mr.config.IS_DEBUG is True:
            reduce_result_gen = list(reduce_result_gen)            
            _logger.debug("Handler [%s] reduction [%s] result:\n%s", 
                          step.reduce_handler_name, 
                          map_invocation.invocation_id, 
                          pprint.pformat(reduce_result_gen))

        if map_invocation.parent_invocation_id is not None:
            decrement_invocation = mr.models.kv.invocation.get(
                                    workflow, 
                                    map_invocation.parent_invocation_id)
        else:
            decrement_invocation = None

        self.__store_reduction_result(
            message_parameters,
            reduce_result_gen, 
            map_invocation,
            decrement_invocation=decrement_invocation)

    def __store_reduction_result(self, message_parameters, reduce_result_gen,
                                 store_to_invocation, 
                                 decrement_invocation=None):
        """Store the reduction result. This is code common to both/all kinds of 
        reduction.
        """

        workflow = message_parameters.workflow
        request = message_parameters.request

        # Store result

        _logger.debug("Storing reduction result: "
                      "[%s] [%s]", 
                      store_to_invocation.invocation_id,
                      store_to_invocation.direction)

        dq = mr.models.kv.queues.dataset.DatasetQueue(
                workflow, 
                store_to_invocation,
                mr.models.kv.queues.dataset.DT_POST_REDUCE)

        for (k, v) in reduce_result_gen:
            data = {
                # Pair
                'p': (k, v),
            }

            dq.add(data)

        _logger.debug("We've posted the reduction result to invocation: "
                      "[%s]", store_to_invocation.invocation_id)

        if decrement_invocation is not None:
            _logger.debug("Decrementing invocation: [%s] WAITING=(%d)",
                          decrement_invocation.invocation_id,
                          decrement_invocation.mapped_waiting)

            # Decrement the "waiting" counter on the parent of the parent 
            # (the step that mapped the steps that produced the results 
            # that we're reducing), or notify that the job is done (if 
            # there is no parent's parent).

            decrement_invocation = self.__decrement_invocation(
                                    workflow, 
                                    decrement_invocation)

            if decrement_invocation.mapped_waiting == 0:
                # We've posted the reduction of the results of our map step 
                # to its parent, and all mapped steps of that parent have 
                # now been reported.

                _logger.debug("Invocation [%s] mapped-waiting count has "
                              "dropped to (0), and will be reduced.", 
                              decrement_invocation.invocation_id)

                pusher = _get_pusher()

                # Queue a reduction with our parent's parent (the parent of 
                # the original mapping). It will access all of the results 
                # that have been posted back to it.
                pusher.queue_reduce_step_from_parameters(
                    message_parameters, 
                    decrement_invocation)
            else:
                _logger.debug("Invocation [%s] mapped-waiting "
                              "count after REDUCE: (%d)", 
                              decrement_invocation.invocation_id,
                              decrement_invocation.mapped_waiting)
        else:
            # We've reduced our way back up to the original request.

            _logger.debug("No further parents. Marking request as "
                          "complete: [%s]", request.request_id)

            request.done = True
            request.save()

_sp = _StepProcessor()

def get_step_processor():
    return _sp


class _RequestReceiver(object):
    """Receives the web-requests to push new job requests."""

    def __init__(self):
        self.__q = mr.queue.queue_manager.get_queue()

    def __push_request(self, message_parameters):
        pusher = _get_pusher()
        pusher.queue_initial_map_step_from_parameters(message_parameters)

    def __block_for_result(self, message_parameters):
        request = message_parameters.request

        _logger.debug("Blocking on result for request: [%s]", 
                      request.request_id)

        # The object is replaced with a newer one, when a change happens.
        request = request.wait_for_change()

        _logger.debug("Reading result from: [%s] [%s]",
                      message_parameters.invocation.invocation_id,
                      message_parameters.invocation.direction)

        dq = mr.models.kv.queues.dataset.DatasetQueue(
                message_parameters.workflow, 
                message_parameters.invocation, 
                mr.models.kv.queues.dataset.DT_POST_REDUCE)

        results = list(dq.list_data())

        _logger.debug("Retrieved result for request:\n%s", results)

        result_pairs = [d['p'] for d in results]

        if mr.config.IS_DEBUG is True:
            _logger.debug("Result to return for request:\n%s", 
                          pprint.pformat(result_pairs))

        return { 
            'request_id': request.request_id,
            'result': result_pairs,
        }

    def process_request(self, message_parameters):
        self.__push_request(message_parameters)
        r = self.__block_for_result(message_parameters)

        return r

_request_receiver = None
def get_request_receiver():
    global _request_receiver

    if _request_receiver is None:
        _request_receiver = _RequestReceiver()

    return _request_receiver

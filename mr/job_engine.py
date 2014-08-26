import sys
import logging
import functools
import types
import traceback
import json

import gevent.pool

import etcd.exceptions

import mr.config
import mr.config.queue
import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.handler
import mr.models.kv.invocation
import mr.models.kv.trees.mapped_steps
import mr.models.kv.trees.invocations
import mr.queue.queue_manager
import mr.workflow_manager
import mr.shared_types
import mr.constants

_logger = logging.getLogger(__name__)

# TODO(dustin): We can maintain a nice little cache if the whole cluster pushes 
#               updates to it.

# TODO(dustin): We still have to deal with KV changes not yet having propagated 
#               by the time a queued message is picked-up by a worker.

# TODO(dustin): We might push changes into both *etcd* and *memcache*. Since 
#               there might be only one instance of memcache (and entirely in 
#               memory) vs many instances of *etcd* (and entirely on disk), it 
#               might provide us immediate concurrency while not completely 
#               sacrificing durability (the process going down, there should 
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

        parent_tree.add_child(reduce_invocation.invocation_id)

        # Queue the reduction.
# TODO(dustin): We would prefer to derive the message-parameters *from* the 
#               invocation, but that would require several lookups when we 
#               already have the data right here.
        reduce_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            workflow=workflow,
            invocation=reduce_invocation,
            request=message_parameters.request,
            job=message_parameters.job,
            step=reduce_step,
            handler=reduce_step.reduce_handler_name,
            arguments=None)

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

    def __queue_map_step(self, mapped_steps_tree, next_step, next_arguments, 
                         original_parameters):
        request = original_parameters.request
        workflow = original_parameters.workflow
        job = original_parameters.job
        step = original_parameters.step
        invocation = original_parameters.invocation

        _logger.debug("Queueing mapped step from invocation [%s] and step [%s] to next (downstream) step [%s]",
                      invocation.invocation_id, step.step_name, next_step.step_name)

        next_handler = mr.models.kv.handler.get(
                            workflow, 
                            next_step.map_handler_name)
        
        next_arguments = dict(next_handler.cast_arguments(
                                            next_arguments))

        # The next invocation will have this [mapping] step as a parent.
        map_invocation = mr.models.kv.invocation.Invocation(
                                invocation_id=None,
                                workflow_name=workflow.workflow_name,
                                parent_invocation_id=invocation.invocation_id,
                                step_name=next_step.step_name,
                                arguments=next_arguments,
                                direction=mr.constants.D_MAP)

# TODO(dustin): Debugging.
        assert map_invocation.parent_invocation_id is not None

        map_invocation.save()

        # Create a tracking tree for this new invocation.

        it = mr.models.kv.trees.invocations.InvocationsTree(
                workflow, 
                map_invocation)

        it.create()

        # Add an association to bind this invocation to our parent's.

        parent_tree = mr.models.kv.trees.invocations.InvocationsTree(
                        workflow, 
                        invocation)

        parent_tree.add_child(map_invocation.invocation_id)

        # Queue the mapping.

        _logger.debug("Mapped invocation: [%s] => [%s]",
                      invocation.invocation_id, map_invocation.invocation_id)

        mapped_steps_tree.add_child(map_invocation.invocation_id)

        mapped_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            workflow=workflow,
            invocation=map_invocation,
            request=request,
            job=job,
            step=next_step,
            handler=next_handler,
            arguments=next_arguments)

        _get_pusher().queue_map_step_from_parameters(mapped_parameters)

    def __map_downstream(self, handler_name, handler_result_gen,
                         workflow, invocation, message_parameters):
        """A mapping step has completed and has mapped into one or more 
        downstream steps. Queue the downstream steps to be handled and tracked.
        """

        assert invocation.mapped_count is None
        assert invocation.mapped_waiting is None

        # This has to be an integer just in case one of the downstream steps 
        # completes before we finish our accounting, here.

        invocation.mapped_waiting = 0
        invocation.save()

        # Create a home for the mapping tree, for the parent's invocation 
        # record.

        mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                workflow, 
                invocation)

        mst.create()

        i = 0
        for (mapped_step_name, mapped_arguments) in handler_result_gen:
            mapped_step = mr.models.kv.step.get(
                            workflow, 
                            mapped_step_name)
# TODO(dustin): !! We're not guaranteed an order when retrieving this. 
#               Hopefully, we can refactor to use in-order queues. Otherwise, 
#               we'll have to store the indices, which we won't be able to 
#               reassemble the data with efficiently.
            self.__queue_map_step(
                    mst,
                    mapped_step, 
                    mapped_arguments, 
                    message_parameters)

            i += 1

        step_count = i

        # Now, update the number of mapped steps into the invocation. Because 
        # we either decrement or add maximum positive value, and the value of 
        # mapped_waiting will never be glimpsed before decrementing it, there 
        # won't be any chance of a completed step seeing the mappd_waiting 
        # value equal zero more than once (which is our trigger for a 
        # reduction), which will be the very last manipulation it counters.

        _logger.debug("Invocation [%s] has mapped (%d) steps.", 
                      invocation.invocation_id, step_count)
# We might need to check for whether a reduction is necessary here. By the time 
# we get here, we could've potentially finished all steps, which nothing else 
# checking for (0) waiting-steps.
        invocation = self.__add_mapped_steps(
                        workflow, 
                        invocation, 
                        step_count)

        _logger.debug("Invocation [%s] has had its counts updated: MC=(%d) "
                      "MW=(%d)", 
                      invocation.invocation_id, invocation.mapped_count, 
                      invocation.mapped_waiting)

    def handle_map(self, message_parameters):
        """Handle one dequeued map job."""

        request = message_parameters.request
        step = message_parameters.step
        invocation = message_parameters.invocation
        workflow = message_parameters.workflow
        
        wm = mr.workflow_manager.get_wm()
        managed_workflow = wm.get(workflow.workflow_name)
        handlers = managed_workflow.handlers

        _logger.debug("Processing MAP: [%s]", invocation.invocation_id)

        try:
            # Call the handler.
            
            handler_result = handlers.run_handler(
                                step.map_handler_name, 
                                message_parameters.arguments)

            _logger.debug("Mapper result [%s]: [%s]", 
                          handler_result.__class__.__name__, handler_result)

            # Manage downstream steps that were mapped to (the handler was a 
            # generator).

            if issubclass(
               handler_result.__class__, 
               types.GeneratorType) is True:
                self.__map_downstream(
                    step.map_handler_name, 
                    handler_result,
                    workflow, 
                    invocation, 
                    message_parameters)
            else:
                # The step didn't yield, so it must've done some work and 
                # returned a result. Store it.

                if invocation.parent_invocation_id is not None:
                    # We were mapped-to from another invocation.

                    _logger.debug("Storing result to parent-invocation with ID: [%s]", 
                                  invocation.parent_invocation_id)

                    parent_invocation = mr.models.kv.invocation.get(
                                            workflow, 
                                            invocation.parent_invocation_id)

                    mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                            workflow, 
                            parent_invocation)

                    meta = {
                        'result': handler_result,
                    }

                    mst.update_child(invocation.invocation_id, meta=meta)

                    # Decrement the "waiting" counter on the parent.

                    parent_invocation = self.__decrement_parent_invocation(
                                            workflow,
                                            invocation)

                    # It might be negative if the total number of steps has 
                    # already been added to it or negative if not and steps are 
                    # already being processed, but, if it's exactly zero, we're 
                    # done.
                    if parent_invocation.mapped_waiting == 0:
                        _logger.debug("REFLECTION: Handler for MAPPING "
                                      "returned a result, and all (%d) "
                                      "results have been rendered for parent. "
                                      "Reducing for parent [%s].", 
                                      parent_invocation.mapped_count, 
                                      parent_invocation.invocation_id)

                        pusher = _get_pusher()
                        pusher.queue_reduce_step_from_parameters(
                            message_parameters, 
                            parent_invocation)
                    else:
                        _logger.debug("Parent invocation [%s] mapped-waiting "
                                      "count after ACTION: (%d)", 
                                      parent_invocation.invocation_id, 
                                      parent_invocation.mapped_waiting)
# TODO(dustin): Just for debugging.
                        mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                                workflow, 
                                parent_invocation)

                        children_count = len(list(mst.list_entities_and_meta()))

                        _logger.debug("Stored results: (%d)", children_count)
                else:
                    # We were called directly from the initial-step of the 
                    # request. There is no tree of results. There was no 
                    # mapping. Post the one result to the request.

                    _logger.debug("Storing unmapped handler result to "
                                  "request: [%s]", handler_result)

                    request.result = handler_result
                    request.save()
        except:
# TODO(dustin): Whatever is checking for a result needs to be notified about a breakdown.
# TODO(dustin): We might have to remove the chain of invocations, on error.
            invocation.error = traceback.format_exc()
# TODO(dustin): This is trying to create the record.
            invocation.save()

            raise

    def __decrement_parent_invocation(self, workflow, invocation):
        def get_cb():
            return mr.models.kv.invocation.get(
                    workflow, 
                    invocation.parent_invocation_id)

        def set_cb(obj):
            obj.mapped_waiting -= 1

        return mr.models.kv.invocation.Invocation.atomic_update(get_cb, set_cb)

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
        invocation = message_parameters.invocation
        workflow = message_parameters.workflow
        request = message_parameters.request

        assert step.reduce_handler_name is not None

        wm = mr.workflow_manager.get_wm()
        managed_workflow = wm.get(workflow.workflow_name)
        handlers = managed_workflow.handlers

        # The parent of the current invocation is the invocation that had all 
        # of the mappings to be reduced.

        parent_invocation = mr.models.kv.invocation.get(
                                workflow, 
                                invocation.parent_invocation_id)

        _logger.debug("Processing REDUCE [%s] of original invocation [%s].", 
                      invocation.invocation_id, parent_invocation.invocation_id)

        try:
            # Call the handler with a generator of all of the results to be 
            # reduced.

            mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                    workflow, 
                    parent_invocation)

            children_gen = mst.list_entities_and_meta()

            if mr.config.IS_DEBUG is True:
                children_gen = list(children_gen)

                _logger.debug("(%d) results will be reduced by step [%s] for "
                              "original invocation [%s].",
                              len(children_gen), step.reduce_handler_name, 
                              parent_invocation.invocation_id)
                
                print('')
                for (i, (child_invocation, encoded_meta)) \
                    in enumerate(children_gen):
                        meta = json.loads(encoded_meta)
                        print("Result (%d): [%s] %s" % 
                              (i, child_invocation.invocation_id, 
                               meta['result']))

                print('')

# TODO(dustin): Our mechanics still aren't guaranteeing the order of the results.
            results_gen = (json.loads(encoded_meta)['result']
                           for (child_invocation, encoded_meta) 
                           in children_gen)

            arguments = {
                'results': results_gen,
            }

            handler_result = handlers.run_handler(
                                step.reduce_handler_name, 
                                arguments)

            _logger.debug("Handler [%s] reduction [%s] result: [%s]", 
                          step.reduce_handler_name, invocation.invocation_id, 
                          handler_result)

            if parent_invocation.parent_invocation_id is not None:
                parent_parent_invocation = mr.models.kv.invocation.get(
                                            workflow, 
                                            parent_invocation.parent_invocation_id)

                # Store the result from the reduction of mapped steps of our 
                # parent, to *its* parent.

                mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                        workflow, 
                        parent_parent_invocation)

                meta = {
                    'result': handler_result,
                }

                mst.update_child(parent_invocation.invocation_id, meta=meta)

                # Decrement the "waiting" counter on the parent, or notify that 
                # the job is done.

                self.__decrement_parent_invocation(
                    workflow,
                    parent_invocation)

                if parent_parent_invocation.mapped_waiting == 0:
                    _logger.debug("Handler for REDUCTION returned a result, "
                                  "and all results have been rendered for "
                                  "parent-parent. Reducing for parent [%s].", 
                                  parent_invocation.invocation_id)

                    _get_pusher().queue_reduce_step_from_parameters(
                        message_parameters, 
                        parent_parent_invocation)
                else:
                    _logger.debug("Parent invocation [%s] mapped-waiting "
                                  "count after REDUCE: (%d)", 
                                  parent_parent_invocation.invocation_id,
                                  parent_parent_invocation.mapped_waiting)
            else:
                # We've reduced our way back up to the original request.

                _logger.debug("Storing final reduction result to request: "
                              "[%s]", handler_result)

                request.result = handler_result
                request.save()
        except:
# TODO(dustin): Whatever is checking for a result needs to be notified about a breakdown.
# TODO(dustin): We might have to remove the chain of invocations, on error.
            invocation.error = traceback.format_exc()
            invocation.save()

            raise

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

        r = request.wait_for_change()

        return { 
            'request_id': request.request_id,
            'result': r.result,
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

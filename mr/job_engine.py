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
import mr.queue.queue_manager
import mr.workflow_manager
import mr.shared_types
import mr.constants

_logger = logging.getLogger(__name__)


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

        _logger.debug("Queueing reduce step for parent invocation: %s", 
                      parent_invocation)

        reduce_invocation = mr.models.kv.invocation.Invocation(
                                invocation_id=None,
                                workflow_name=\
                                    message_parameters.workflow.workflow_name,
                                parent_invocation_id=\
                                    parent_invocation.invocation_id,
                                step_name=parent_invocation.step_name,
                                direction=mr.constants.D_REDUCE)

        reduce_invocation.save()

        reduce_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            workflow=message_parameters.workflow,
            invocation=reduce_invocation,
            request=message_parameters.request,
            job=message_parameters.job,
            step=message_parameters.step,
            handler=message_parameters.step.reduce_handler_name,
            arguments=None)

        replacements = {
            'workflow_name': reduce_parameters.workflow.workflow_name,
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
        invocation = original_parameters.invocation

        _logger.debug("Queueing mapped step:\n%s =>\n%s",
                      invocation, next_step)

        next_handler = mr.models.kv.handler.get(
                            workflow, 
                            next_step.map_handler_name)
        
        next_arguments = dict(next_handler.cast_arguments(
                                            next_arguments))

        # The next invocation will have this [mapping] step as a parent.
        mapped_invocation = mr.models.kv.invocation.Invocation(
                                invocation_id=None,
                                workflow_name=workflow.workflow_name,
                                parent_invocation_id=invocation.invocation_id,
                                step_name=next_step.step_name,
                                arguments=next_arguments,
                                direction=mr.constants.D_MAP)

# TODO(dustin): Debugging.
        assert mapped_invocation.parent_invocation_id is not None

        mapped_invocation.save()

        _logger.debug("Mapped invocation:\n%s =>\n%s",
                      invocation, mapped_invocation)

        mapped_steps_tree.add_child(mapped_invocation.invocation_id)

        mapped_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            workflow=workflow,
            invocation=mapped_invocation,
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

        # The handler must first yield the number of steps that will be
        # mapped-to.

        try:
            step_count = handler_result_gen.next()
        except StopIteration:
            _logger.error("Handler returned an empty generator. "
                          "Weird: [%s]", handler_name)
            raise

        _logger.debug("Invocation [%s] has mapped (%d) steps.", 
                      invocation.invocation_id, step_count)

        if issubclass(step_count.__class__, int) is False:
            raise ValueError("We expect an integer step count from "
                             "handler [%s], but didn't get it: [%s]" %
                             (handler_name, step_count))

        # Create a home for the mapping tree, for the parent's invocation 
        # record.

        mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                workflow, 
                invocation)

        mst.create()

        # Post the number of steps that were mapped before we do the 
        # actual queueing, so our counters decrement correctly.

        invocation.mapped_count = step_count
        invocation.mapped_waiting = step_count
        invocation.save()

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

        if i != step_count:
            raise ValueError("The number of steps (%d) yielded by "
                             "handler [%s] did not match the "
                             "announced count (%d)." % 
                             (i, handler_name, step_count))

    def handle_map(self, message_parameters):
        """Handle one dequeued map job."""

        request = message_parameters.request
        step = message_parameters.step
        invocation = message_parameters.invocation
        workflow = message_parameters.workflow
        
        wm = mr.workflow_manager.get_wm()
        managed_workflow = wm.get(workflow.workflow_name)
        handlers = managed_workflow.handlers

        _logger.debug("Processing MAP: %s", invocation)

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

                    if parent_invocation.mapped_waiting <= 0:
                        _logger.debug("REFLECTION: Handler for MAPPING "
                                      "returned a result, and all results "
                                      "have been rendered for parent. "
                                      "Reducing for parent [%s].", 
                                      parent_invocation.invocation_id)

                        pusher = _get_pusher()
                        pusher.queue_reduce_step_from_parameters(
                            message_parameters, 
                            parent_invocation)
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

        wm = mr.workflow_manager.get_wm()
        managed_workflow = wm.get(workflow.workflow_name)
        handlers = managed_workflow.handlers

        # The parent of the current invocation is the invocation that had all 
        # of the mappings to be reduced.

        parent_invocation = mr.models.kv.invocation.get(
                                workflow, 
                                invocation.parent_invocation_id)

        _logger.debug("Processing REDUCE [%s] of original invocation [%s].", 
                      invocation, parent_invocation)

        try:
            # Call the handler with a generator of all of the results to be 
            # reduced.

            mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                    workflow, 
                    parent_invocation)

            children_gen = mst.list_entities()

            if mr.config.IS_DEBUG is True:
                children_gen = list(children_gen)
                
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

                if parent_parent_invocation.mapped_waiting <= 0:
                    _logger.debug("Handler for REDUCTION returned a result, "
                                  "and all results have been rendered for "
                                  "parent-parent. Reducing for parent [%s].", 
                                  parent_invocation.invocation_id)

                    _get_pusher().queue_reduce_step_from_parameters(
                        message_parameters, 
                        parent_parent_invocation)
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
# TODO(dustin): Finish.
# TODO(dustin): Come back to this once this is necessary.

        return { 
            'request_id': message_parameters.request.request_id,
            'placeholder': '(should block for result)',
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

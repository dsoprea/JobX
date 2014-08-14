import sys
import logging
import functools
import types
import traceback

import gevent.pool

import etcd.exceptions

import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.handler
import mr.models.kv.invocation
import mr.models.kv.trees.mapped_steps
import mr.queue_manager
import mr.workflow_manager
import mr.shared_types
import mr.constants

_logger = logging.getLogger(__name__)


class _QueuePusher(object):
    def __init__(self):
        self.__q = mr.queue_manager.get_queue()

    def queue_map_step_from_parameters(self, message_parameters):
# TODO(dustin): We should move all of this logic into a general 'job control' 
#               class that can be used for incoming requests as well as from 
#               yields in the handlers.
# TODO(dustin): We might increment the number of total steps processed on the 
#               request.

        topic = 'mr.%s.map' + message_parameters.request.workflow_name

        self.__q.producer.push_one(
            topic, 
            mr.constants.D_MAP, 
            message_parameters)

    def queue_initial_map_step_from_parameters(self, message_parameters):
        return self.queue_map_step_from_parameters(message_parameters)

    def queue_reduce_step_from_parameters(self, message_parameters):
# TODO(dustin): We should move all of this logic into a general 'job control' 
#               class that can be used for incoming requests as well as from 
#               yields in the handlers.

        topic = 'mr.%s.reduce' + message_parameters.request.workflow_name

        self.__q.producer.push_one(
            topic, 
            mr.constants.D_REDUCE, 
            message_parameters)

    def queue_initial_reduce_step_from_parameters(self, message_parameters, 
                                                  parent_invocation):
        """We're reflecting (switch directions from mapping to reduction). The 
        current step is an action step (no mappings were done). The next 
        invocation will successively take the invocation-IDs of one parent to 
        the next.
        """

        reduce_invocation = mr.models.kv.invocation.Invocation(
                                parent_invocation_id=\
                                    parent_invocation.invocation_id,
                                step_name=parent_invocation.step_name,
                                direction=mr.constants.D_REDUCE)

        reduce_invocation.save()

        reduce_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            managed_workflow=message_parameters.managed_workflow,
            invocation=reduce_invocation,
            request=message_parameters.request,
            job=message_parameters.job,
            step=message_parameters.step,
            handler=message_parameters.step.reduce_handler_name,
            arguments=None)

        return self.queue_reduce_step_from_parameters(reduce_parameters)

_qp = _QueuePusher()


class _StepProcessor(object):
    """Receives queued items to be processed. We are running in our own gthread 
    by the time we're called.
    """

    def __queue_map_step(self, mapped_steps_tree, next_step, next_arguments, 
                         original_parameters):
        request = original_parameters.request
        managed_workflow = original_parameters.managed_workflow
        workflow = managed_workflow.workflow
        job = original_parameters.job
        invocation = original_parameters.invocation

        _logger.debug("Queueing mapped step:\n%s =>\n%s",
                      invocation, next_step)

        next_handler = mr.models.kv.handler.get(
                            workflow, 
                            next_step.map_handler_name)
        
        next_arguments = next_handler.cast_arguments(
                            next_arguments)

        # The next invocation will have this [mapping] step as a parent.
        mapped_invocation = mr.models.kv.invocation.Invocation(
                            parent_invocation_id=invocation.invocation_id,
                            step_name=next_step.step_name,
                            arguments=next_arguments,
                            direction=mr.constants.D_MAP)

        mapped_invocation.save()

        _logger.debug("Mapped invocation:\n%s =>\n%s",
                      invocation, mapped_invocation)

        mapped_steps_tree.add_child(mapped_invocation.invocation_id)

        mapped_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            managed_workflow=managed_workflow,
            invocation=mapped_invocation,
            request=request,
            job=job,
            step=next_step,
            handler=next_handler,
            arguments=next_arguments)

        _qp.queue_map_step_from_parameters(mapped_parameters)

#    def __queue_reduce_step(self, next_step, next_arguments, original_parameters):
#        request = original_parameters.request
#        managed_workflow = original_parameters.managed_workflow
#        workflow = managed_workflow.workflow
#        job = original_parameters.job
#        invocation = original_parameters.invocation
#
#        _logger.debug("Queueing mapped step:\n%s =>\n%s",
#                      invocation, next_step)
#
#        next_handler = mr.models.kv.handler.get(
#                            workflow, 
#                            next_step.map_handler_name)
#        
#        next_arguments = next_handler.cast_arguments(
#                            next_arguments)
#
#        # The next invocation will have this [mapping] step as a parent.
#        mapped_invocation = mr.models.kv.invocation.Invocation(
#                            parent_invocation_id=invocation.invocation_id,
#                            step_name=next_step.step_name,
#                            arguments=next_arguments,
#                            direction=mr.constants.D_MAP)
#
#        mapped_invocation.save()
#
#        _logger.debug("Mapped invocation:\n%s =>\n%s",
#                      invocation, mapped_invocation)
#
#        reduce_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
#            managed_workflow=managed_workflow,
#            invocation=mapped_invocation,
#            request=request,
#            job=job,
#            step=next_step,
#            handler=next_handler,
#            arguments=next_arguments)
#
#        _qp.queue_reduce_step_from_parameters(reduce_parameters)

    def __register_downstream_mappings(self, handler_name, handler_result_gen,
                                       workflow, invocation, 
                                       message_parameters):
        """A mapping step has completed. Queue the downstream steps to be 
        handled and tracked.
        """

        # The handler must first yield the number of steps that will be
        # mapped-to.

        try:
            step_count = handler_result_gen.next()
        except StopIteration:
            _logger.error("Handler returned an empty generator. "
                          "Weird: [%s]", handler_name)
            raise

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

    def handle_map(self, message_handler, message_parameters):
        """Handle one dequeued map job."""

        request = message_parameters.request
        job = message_parameters.job
        step = message_parameters.step
        invocation = message_parameters.invocation
        managed_workflow = message_parameters.managed_workflow
        workflow = managed_workflow.workflow

        handler_name = step.map_handler_name
        handlers = managed_workflow.handlers

        try:
            # Call the handler.
            
            handler_result = handlers.run_handler(
                                handler_name, 
                                message_parameters.arguments)

            # Manage downstream steps that were mapped to (the handler was a 
            # generator).

            if issubclass(
               handler_result.__class__, 
               types.GeneratorType) is True:
                self.__register_downstream_mappings(
                    handler_name, 
                    handler_result,
                    workflow, 
                    invocation, 
                    message_parameters)
            else:
                # The step didn't yield, so it must've done some work and 
                # returned a result. Store it.

                # Post the result.

                mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                        workflow, 
                        invocation.parent_invocation_id)

                meta = {
                    'result': handler_result,
                }

                mst.update_child(invocation.invocation_id, meta=meta)

                # Decrement the "waiting" counter on the parent, or notify that 
                # the job is done.

                if invocation.parent_invocation_id is not None:
                    parent_invocation = self.__decrement_map_parent_invocation(
                                            invocation)

                    if parent_invocation.mapped_waiting <= 0:
                        _qp.queue_initial_reduce_step_from_parameters(
                            message_parameters, 
                            parent_invocation)
                else:
# TODO(dustin): We don't have a parent, and just finished performing an action 
#               (a non-mapping step). This was a request that was fulfilled 
#               immediately. Flag the root invocation (or job?) as complete.
# TODO(dustin): Finish.
                    raise NotImplementedError()
        except:
# TODO(dustin): Whatever is checking for a result needs to be notified about a breakdown.
# TODO(dustin): We might have to remove the chain of invocations, on error.
            invocation.error = traceback.format_exc()
            invocation.save()

            raise

    def __decrement_map_parent_invocation(self, invocation):
        def get_cb():
            return mr.models.kv.invocation.get(
                    invocation.workflow, 
                    invocation.parent_invocation_id)

        def set_cb(obj):
            obj.mapped_waiting -= 1

        return mr.models.kv.invocation.Invocation.atomic_update(get_cb, set_cb)

    def handle_reduce(self, message_handler, message_parameters):
        """Corresponds to steps received with a type of mr.constants.D_REDUCE.
        """

        mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                message_parameters.workflow, 
                message_parameters.invocation.parent_invocation_id)

        results_gen = mst.list_entities()


# 1. Create a generator that reads down the MappedStep tree.
# 2. Call the reduction handler -with- the generator.
#    * We might want to create the generator in whatever process/thread is 
#      running the handler so that we can be configured to use multiprocessing 
#      (we can pas a generator via IPC).
# 3. Post the result.
# 4. After it completes, create an invocation record for the parent.

# TODO(dustin): Finish.

# TODO(dustin): We'll need to create an invocation record if/when we have to queue a reduction step.


#                mst = mr.models.kv.trees.mapped_steps.MappedStepsTree(
#                        workflow, 
#                        invocation.parent_invocation_id)
#
#                meta = {
#                    'result': handler_result,
#                }
#
#                mst.update_child(invocation.invocation_id, meta=meta)


        raise NotImplementedError()

_sp = _StepProcessor()

def get_step_processor():
    return _jd


class _RequestReceiver(object):
    """Receives the web-requests to push new job requests."""

    def __init__(self):
        self.__q = mr.queue_manager.get_queue()
        self.__wm = mr.workflow_manager.get_wm()

    def __push_request(self, message_parameters):
        _qp.queue_initial_map_step_from_parameters(message_parameters)

    def __block_for_result(self, request):
# TODO(dustin): Finish.
# TODO(dustin): Come back to this once this is necessary.
        pass
#        raise NotImplementedError()

    def process_request(self, request):
        self.__push_request(request)
        r = self.__block_for_result(request)

        return r

_rr = _RequestReceiver()

def get_request_receiver():
    return _rr

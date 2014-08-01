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
import mr.queue_manager
import mr.workflow_manager
import mr.shared_types

_logger = logging.getLogger(__name__)

# Step types
ST_MAP = 'map'
ST_REDUCE = 'reduce'

def _queue_map_step_from_parameters(message_parameters):
# TODO(dustin): We should move all of this logic into a general 'job control' 
#               class that can be used for incoming requests as well as from 
#               yields in the handlers.
# TODO(dustin): We might increment the number of total steps processed on the 
#               request.

    topic = 'mr.%s' + message_parameters.request.workflow_name

# TODO(dustin): Make "mapper" a constant.
    self.__q.producer.push_one(topic, ST_MAP, message_parameters)


class _StepProcessor(object):
    """Receives queued items to be processed. We are running in our own gthread 
    by the time we're called.
    """

    def __queue_next_step(self, next_step, next_arguments, original_parameters, 
                          step_type):
        request = original_parameters.request
        managed_workflow = original_parameters.managed_workflow
        workflow = managed_workflow.workflow
        job = original_parameters.job
        invocation = original_parameters.invocation

        if step_type == ST_MAP:
            next_handler_name = next_step.map_handler_name
        elif step_type == ST_REDUCE:
            next_handler_name = next_step.reduce_handler_name
        else:
            raise ValueError("Step type is invalid: [%s]" % (step_type,))

        next_handler = mr.models.kv.handler.get(
                            workflow, 
                            next_handler_name)
        
        next_arguments = next_handler.cast_arguments(
                                            next_arguments)

        next_invocation = mr.models.kv.invocation.Invocation(
                                parent_invocation_id=\
                                    invocation.invocation_id,
                                step_name=next_step.step_name,
                                arguments=next_arguments,
                            )

        next_invocation.save()

        next_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            managed_workflow=managed_workflow,
            invocation=next_invocation,
            request=request,
            job=job,
            step=next_step,
            handler=next_handler,
            arguments=next_arguments)

        _queue_map_step_from_parameters(next_parameters)

    def handle_map(self, message_handler, message_parameters):
        """Corresponds to steps received with a type of ST_MAP."""

        request = message_parameters.request
        job = message_parameters.job
        step = message_parameters.step
        invocation = message_parameters.invocation
        managed_workflow = message_parameters.managed_workflow
        workflow = managed_workflow.workflow

        handler_name = step.map_handler_name
        handlers = managed_workflow.handlers

        try:
            r = handlers.run_handler(
                    handler_name, 
                    message_parameters.arguments)

            if issubclass(r.__class__, types.GeneratorType) is True:
                # The handler must first yield the number of steps that will be 
                # mapped-to.

                try:
                    step_count = r.next()
                except StopIteration:
                    _logger.error("Handler returned an empty generator. "
                                  "Weird: [%s]", handler_name)
                    raise

                if issubclass(step_count.__class__, int) is False:
                    raise ValueError("We expect an integer step count from "
                                     "handler [%s], but didn't get it: [%s]" %
                                     (handler_name, step_count))

                # Post the number of steps that were mapped before we do the 
                # actual queueing, so our counters decrement correctly.

                invocation.mapped_count = step_count
                invocation.mapped_waiting = step_count
                invocation.save()

                i = 0
                for (mapped_step, mapped_arguments) in steps:
                    self.__queue_next_step(
                        mapped_step, 
                        mapped_arguments, 
                        message_parameters,
                        ST_MAP)

                    i += 1

                if i != step_count:
                    raise ValueError("The number of steps (%d) yielded by "
                                     "handler [%s] did not match the "
                                     "announced count (%d)." % 
                                     (i, handler_name, step_count))
            else:
                # The step didn't yield, so it must've done some work and 
                # returned a result. Store it.
                #
                # Since the counter might be updated by something else running 
                # concurrently, we'll ask for failure if it's been changed 
                # since we loaded it, and retry until successful.

                invocation.result = r
                invocation.save()

                self.__decrement_parent_invocation(invocation)

# TODO(dustin):
#   Process:
#       1. Get parent invocation record.
#       2. Get the step for the parent invocation record.
#       3. Collect the results for all steps mapped from the parent invocation. 
#          We will pass a generator to the reduction handler.
#
# TODO(dustin): Can we get a list of keys from the server? Can we iteratively read them, rather than read all of them at once?
                    pass

#                    self.__queue_next_step(
#                        mapped_step, 
#                        mapped_arguments, 
#                        message_parameters,
#                        ST_REDUCE)
        except:
            invocation.error = traceback.format_exc()
            invocation.save()

            raise

    def __queue_reduce_step(self, parent_invocation):
# TODO(dustin): Finish.

#        result_gen = mr.models.kv.invocation.Invocation.list_keys(
#                        workflow.workflow_name, 
#                        invocation.invocation_id,
#                        )
        raise NotImplementedError()

    def __decrement_map_parent_invocation(self, invocation):

        while 1:
            parent_invocation = mr.models.kv.invocation.get(
                                    invocation.workflow, 
                                    invocation.parent_invocation_id)

            try:
                parent_invocation.mapped_waiting -= 1
                parent_invocation.save(check_index=True)
            except etcd.exceptions.EtcdPreconditionException:
                parent_invocation.refresh()
            else:
                break

        if parent_invocation.mapped_waiting <= 0:
            self.__queue_reduce_step(parent_invocation)

    def handle_reduce(self, message_handler, message_parameters):
        """Corresponds to steps received with a type of ST_REDUCE."""
# TODO(dustin): Finish.

# TODO(dustin): We'll need to create an invocation record if/when we have to queue a reduction step.

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
        _queue_map_step_from_parameters(message_parameters)

    def __block_for_result(self, request):
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

import sys
import logging
import functools
import types

import gevent.pool

import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.handler
import mr.models.kv.invocation
import mr.queue_manager
import mr.workflow_manager
import mr.shared_types

_logger = logging.getLogger(__name__)

def _queue_map_step_from_parameters(message_parameters):
# TODO(dustin): We should move all of this logic into a general 'job control' 
#               class that can be used for incoming requests as well as from 
#               yields in the handlers.
# TODO(dustin): We might increment the number of total steps processed on the 
#               request.

    topic = 'mr.%s' + message_parameters.request.workflow_name

# TODO(dustin): Make "mapper" a constant.
    self.__q.producer.push_one(topic, 'mapper', message_parameters)

class _StepProcessor(object):
    """Receives queued items to be processed. We are running in our own gthread 
    by the time we're called.
    """

    def __queue_map_step(self, mapped_step, mapped_arguments, original_parameters):
        request = original_parameters.request
        managed_workflow = original_parameters.managed_workflow
        workflow = managed_workflow.workflow
        job = original_parameters.job
        invocation = original_parameters.invocation

        mapped_handler = mr.models.kv.handler.get(
                            workflow, 
                            mapped_step.map_handler_name)
        
        mapped_arguments = mapped_handler.cast_arguments(
                                            mapped_arguments)

        mapped_invocation = mr.models.kv.invocation.Invocation(
                                parent_invocation_id=\
                                    invocation.invocation_id,
                                step_name=mapped_step.step_name,
                                arguments=mapped_arguments,
                            )

        mapped_invocation.save()

        mapped_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
            managed_workflow=managed_workflow,
            invocation=mapped_invocation,
            request=request,
            job=job,
            step=mapped_step,
            handler=mapped_handler,
            arguments=mapped_arguments)

        _queue_map_step_from_parameters(mapped_parameters)

    def __queue_reduce_step(self, message_parameters):
# TODO(dustin): Finish.
        raise NotImplementedError()

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
# TODO(dustin): We need to avoid having to flatten the list of steps in order 
#               to get the count. If we have to map into 100,000 downstream 
#               steps, the value of mapreduce will have been lost.
#
#               We need to wrote the invocation records, and *then* queue them,
#               since the information to be queued should be considerably 
#               lighter.
#               
#               We might need to encode down to the raw queue messages from one 
#               to the next, because that's the only point at which we dump the 
#               potential girth of the "message parameters" object (which 
#               describes all of the data for a step, including the arguments).
                steps = list(r)

                # Post the number of steps that were mapped before we do the 
                # actual queueing, so our counters decrement correctly.

                step_count = len(steps)
                invocation.mapped_count = step_count
                invocation.mapped_waiting = step_count
                invocation.save()

                for (mapped_step, mapped_arguments) in steps:
                    self.__queue_map_step(
                        mapped_step, 
                        mapped_arguments, 
                        message_parameters)
            else:
                # The step didn't yield, so it must've done some work and 
                # returned a result. Store it.

                invocation.result = r
# TODO(dustin): We need to use CAS functionality, here: Maybe we can pass in a 
#               custom condition. As we're using JSON bodies, it might be 
#               simpler to use indexes (store the index from the retrieve 
#               inside all models, and allow us to not change a key unless the 
#               index still matches).
                invocation.mapped_waiting -= 1
                invocation.save()

                if invocation.mapped_waiting <= 0:
                    self.__queue_reduce_step(message_parameters)

        except Exception as e:
# TODO(dustin): Store the trace, too.
            invocation.error = str(e)
            invocation.save()

            raise

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

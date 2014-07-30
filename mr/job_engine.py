import sys
import logging
import functools

import gevent.pool

import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.handler
import mr.queue_manager
import mr.workflow_manager

_logger = logging.getLogger(__name__)


class JobEngine(object):
    def __init__(self, workflow):
        self.__workflow = workflow

        s = mr.models.kv.step.StepKv()
        self.__step_library = s.get_library_for_workflow(workflow)

    def run(self):
        """Process a queued job."""

# TODO(dustin): Start the NSQ client as a consumer.

        raise NotImplementedError()

    def queue(self, request, job, arguments):

        distilled_arguments = {}
        for k, v in arguments.items():
            cls = getattr(
                    sys.modules['__builtin__'], 
                    step.argument_spec[k])

            distilled_arguments[k] = cls(v)

# TODO(dustin): Finish pushing into queue. Make sure this request has a "state" 
#               entry that we can block on.

        return { 'result': 123 }

    def __spawn_step(self, invoked_steps, name, arguments):
        """Launch a step in parallel. This is run in its own greenlet."""
        
        # invoked_step = mr.invoked_step.create_invoked_step(step, arguments, parent_invocation)
        # invoked_steps.add(invoked_step)

        # Write this to the queue.
        # The result should be persisted and the completion should be announced to the KV.
        # Wait here until the step has been processed (successfully, or with error).

        raise NotImplementedError()

    def __run_step(self, step, arguments):
# TODO(dustin): This should be invoked from the queue.
        invoked_steps = mr.invoked_steps.InvokedSteps()

        cb = functools.partial(self.__spawn_step, invoked_steps)
        gen = self.__handlers.run_handler(
                invoked_step.step_name, 
                invoked_step.arguments)

        g = gevent.pool.Group()
        r = g.imap_unordered(cb, gen)
        list(r)

        return invoked_steps.reduce()

    def run_job(self, request):
        """The system has just received a new job."""

        # Push an entry to KV to track this workflow invocation.
        # Push the step (or invoked step?) to the queue.
        # Block for a final result, and return.

#        step = self.__steps.get_first_step_for_job(request.job_name)


# TODO(dustin): Just update the request with tracking info (if required).
        raise NotImplementedError()

    def announce_job_result(self, reduction):
        """No more reductions are possible. Report the result."""

        raise NotImplementedError()

    @property
    def handlers(self):
        return self.__handlers

    @property
    def steps(self):
        return self.__steps


class _StepProcessor(object):
    """Receives queued items to be processed."""

    def handle_map(self, message_handler, message_parameters):
        """Corresponds to steps received with a type of ST_MAP."""




# TODO(dustin): Finish.
        raise NotImplementedError()

    def handle_reduce(self, message_handler, message_parameters):
        """Corresponds to steps received with a type of ST_REDUCE."""
# TODO(dustin): Finish.
        raise NotImplementedError()

    def handle_action(self, message_handler, message_parameters):
        """Corresponds to steps received with a type of ST_ACTION."""
# TODO(dustin): Finish.
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
# TODO(dustin): We should move all of this logic into a general 'job control' 
#               class that can be used for incoming requests as well as from 
#               yields in the handlers.
# TODO(dustin): We need to add some context information to the request record
#               (like a 'complete' or 'request' key that can be blocked on).
# TODO(dustin): We might increment the number of total steps processed on the 
#               request.

        topic = 'mr.%s' + message_parameters.request.workflow_name
        job_class = message_parameters.step.step_type

        self.__q.producer.push_one(topic, job_class, message_parameters)

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

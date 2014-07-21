import sys
import logging
import functools

import gevent.pool

import nsq.consumer

import mr.models.kv.step

#import mr.handlers
#import mr.steps
#import mr.invoked_step
#import mr.invoked_steps

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
                invoked_step.name, 
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

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


class _StepProcessor(object):
    """Receives queued items to be processed. We are running in our own gthread 
    by the time we're called.
    """

    def handle_map(self, message_handler, message_parameters):
        """Corresponds to steps received with a type of ST_MAP."""

# TODO(dustin): If any yields, than the return is ignored.

        # message_parameters.invocation

# TODO(dustin): Finish.

# TODO(dustin): We'll need to create invocation records for every yield we catch.

        raise NotImplementedError()

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
# TODO(dustin): We should move all of this logic into a general 'job control' 
#               class that can be used for incoming requests as well as from 
#               yields in the handlers.
# TODO(dustin): We might increment the number of total steps processed on the 
#               request.

        topic = 'mr.%s' + message_parameters.request.workflow_name

# TODO(dustin): Make "map" a constant.
        self.__q.producer.push_one(topic, 'mapper', message_parameters)

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

import logging
import functools 
import threading

import mr.job_engine
import mr.queue.queue_message

_logger = logging.getLogger(__name__)


class MessageHandler(object):
    """Received all data coming off the queue."""

    def __init__(self, *args, **kwargs):
        super(MessageHandler, self).__init__(*args, **kwargs)
        self.__sp = mr.job_engine.get_step_processor()

    def classify_message(self, encoded_message):
        qmp = mr.queue.queue_message.get_queue_message_processor()
        (job_class, format_version, decoded_data) = \
            qmp.decode(encoded_message)

        return (job_class, (format_version, decoded_data))

    def __dispatch(self, handler, decoded_data_info):
        (format_version, decoded_data) = decoded_data_info

        _logger.debug("Dispatching dequeued message: (%d) %s", 
                      format_version, decoded_data)

        qmf = mr.queue.queue_message.get_queue_message_funnel()
        message_parameters = qmf.inflate(format_version, decoded_data)

        t = threading.spawn(target=handler, args=(message_parameters,))
# TODO(dustin): We'll need to join this after the step completes.
        t.start()

    def handle_map(self, connection, message, context):
        """Corresponds to steps received with a type of ST_MAP."""

        handler = functools.partial(self.__sp.handle_map, connection)
        self.__dispatch(handler, context)

    def handle_reduce(self, connection, message, context):
        """Corresponds to steps received with a type of ST_REDUCE."""

        handler = functools.partial(self.__sp.handle_reduce, connection)
        self.__dispatch(handler, context)

import logging
import functools 
import threading

import mr.config.queue
import mr.job_engine
import mr.queue.queue_message

_logger = logging.getLogger(__name__)


class MessageHandler(object):
    """Received all data coming off the queue."""

    def __init__(self, *args, **kwargs):
        super(MessageHandler, self).__init__(*args, **kwargs)
        self.__sp = mr.job_engine.get_step_processor()
        self.__qmp = mr.queue.queue_message.get_queue_message_processor()

    def process_message(self, encoded_message):
        (job_class, format_version, decoded_data) = \
            self.__qmp.decode(encoded_message)

        try:
            handler = getattr(self, 'handle_' + job_class)
        except AttributeError:
            _logger.error("Can't process unhandled job-class: [%s]", 
                          job_class)
            return

        handler(format_version, decoded_data)

    def __dispatch(self, handler, format_version, decoded_message):
        _logger.debug("Dispatching dequeued message: (%d) %s", 
                      format_version, decoded_message)

        qmf = mr.queue.queue_message.get_queue_message_funnel()
        message_parameters = qmf.inflate(format_version, decoded_message)

        if mr.config.queue.IS_MULTITHREADED is True:
            _logger.debug("Dispatching into separate thread.")

            t = threading.Thread(target=handler, args=(message_parameters,))
# TODO(dustin): We'll need to join this after the step completes.
            t.start()
        else:
            _logger.debug("Running message handler synchronously (not "
                          "multithreaded).")

            handler(message_parameters)

    def handle_map(self, format_version, decoded_message):
        """Corresponds to steps received with a type of ST_MAP."""

        self.__dispatch(
                self.__sp.handle_map, 
                format_version, 
                decoded_message)

    def handle_reduce(self, format_version, decoded_message):
        """Corresponds to steps received with a type of ST_REDUCE."""

        self.__dispatch(
                self.__sp.handle_reduce, 
                format_version, 
                decoded_message)

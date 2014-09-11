import logging
import functools 
import threading
import Queue

import mr.config.queue
import mr.job_engine
import mr.queue.queue_message

_logger = logging.getLogger(__name__)


class MessageHandler(object):
    """Received all data coming off the queue. This is a singleton instance."""

    def __init__(self, *args, **kwargs):
        super(MessageHandler, self).__init__(*args, **kwargs)
        self.__sp = mr.job_engine.get_step_processor()
        self.__qmp = mr.queue.queue_message.get_queue_message_processor()

        self.__dispatch_join_q = Queue.Queue()
        self.__dispatch_cleanup_thread_exit_ev = threading.Event()

        self.__schedule_dispatch_cleanup()

    def __del__(self):
        self.__dispatch_cleanup_thread_exit_ev.set()
        self.__dispatch_cleanup_t.join()

    def __schedule_dispatch_cleanup(self):
        self.__dispatch_cleanup_t = threading.Thread(
                                        target=self.__cleanup_dispatches)
        self.__dispatch_cleanup_t.start()

    def __cleanup_dispatches(self):
        while self.__dispatch_cleanup_thread_exit_ev.is_set() is False:
            try:
                dispatched_t = \
                    self.__dispatch_join_q.get(
                        timeout=mr.config.queue.DISPATCH_CLEANUP_INTERVAL_S)
            except Queue.Empty:
                continue

            _logger.debug("Joining finished dispatch: [%s]", dispatched_t.name)
            dispatched_t.join()

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
# TODO(dustin): We should embed KV state, and throw a catchable exception if 
#               the KV has not yet reached consistency. We might add 
#               provisions to take advantage of the indices monotically-
#               increasing nature with *etcd* (whereby we can check whichever 
#               has the highest index first, and if it passes we won't have to 
#               be discretionary towards the rest).
        message_parameters = qmf.inflate(format_version, decoded_message)

        if mr.config.queue.IS_MULTITHREADED is True:
            _logger.debug("Dispatching into separate thread.")

            def wrapped_handler(message_parameters):
                try:
                    handler(message_parameters)
                finally:
                    self.__dispatch_join_q.put(threading.current_thread())

            t = threading.Thread(
                    target=wrapped_handler, 
                    args=(message_parameters,))

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

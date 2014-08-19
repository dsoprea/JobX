import logging
import base64
import threading
import time
import os
import glob
import traceback

import mr.config.fake_queue
import mr.queue.queue_factory
import mr.queue.queue_consumer
import mr.queue.queue_producer
import mr.queue.queue_control
import mr.queue.message_handler

_logger = logging.getLogger(__name__)

_SPOOL_PATH = os.environ['MR_FAKE_QUEUE_SPOOL_PATH']

_FAILED_FILEPATH_TEMPLATE = '%(filepath)s.failed'
_FAILED_MESSAGE_FILEPATH_TEMPLATE = '%(filepath)s.failed.message'
_SUCCESS_FILEPATH_TEMPLATE = '%(filepath)s.success'
_DELAY_AFTER_JOB_HIT = 1
_DELAY_AFTER_JOB_MISS = 2
_SPOOLED_MESSAGE_PATTERN = '*.spooled'


class _FakeMessageHandler(mr.queue.message_handler.MessageHandler):
    pass


class _FakeQueueControl(mr.queue.queue_control.QueueControl):
    """Control interface to the NSQ queue."""

    def __init__(self, p, c):
        super(_FakeQueueControl, self).__init__()

        self.__p = p
        self.__c = c

    def start_producer(self):
        _logger.info("Starting FAKE producer.")

    def start_consumer(self):
        _logger.info("Starting FAKE consumer.")

        self.__c.start_thread()

    def stop_producer(self):
        _logger.info("Stopping FAKE producer.")

    def stop_consumer(self):
        _logger.info("Stopping FAKE consumer.")

        self.__c.stop_thread()


class _FakeQueueProducer(mr.queue.queue_producer.QueueProducer):
    """Producer interface to the NSQ queue."""

    def __init__(self):
        super(_FakeQueueProducer, self).__init__()

    def is_alive(self):
        return True

    def __print_chunks(self, data, chunk_size=80):
        for offset in range(0, len(data), chunk_size):
            print(data[offset:offset + chunk_size])

    def push_one_raw(self, topic, raw_message):
        _logger.debug("Pushing message to topic: [%s]", topic)

        print("PUSH\n  TOPIC: [%s]\n  LEN: (%d)" % (topic, len(raw_message)))
        print('')
        self.__print_chunks(base64.b64encode(raw_message))
        print('')

    def push_many_raw(self, topic, raw_message_list):
        # We can't indicate -how many- messages there are if we were given a 
        # generator.
        _logger.debug("Pushing MANY messages to topic: [%s]", topic)

        print("PUSH\n  TOPIC: [%s]\n  COUNT: (%d)" % 
              (topic, len(list(raw_message_list))))

        for (i, raw_message) in enumerate(raw_message_list):
            print('%d:' % (i,))
            print('')
            self.__print_chunks(base64.b64encode(raw_message))
            print('')


class _FakeQueueConsumer(mr.queue.queue_consumer.QueueConsumer):
    """Consumer interface to the NSQ queue."""

    def __init__(self, topics):
        super(_FakeQueueConsumer, self).__init__()

        self.__topics = topics
        self.__kill_event = threading.Event()
        self.__fmh = _FakeMessageHandler()


    def is_alive(self):
        return True

    def start_thread(self):
        self.__t = threading.Thread(target=self.__thread)
        self.__t.start()

    def stop_thread(self):
        if self.__t.is_alive() is False:
            _logger.error("Can't stop consumer thread. Already not alive.")
            return

        _logger.info("Sending stop to consumer thread.")
        self.__kill_event.set()
        
        _logger.info("Waiting for consumer death.")
        self.__t.join()

    def __read_and_process(self, filename):
        filepath = os.path.join(_SPOOL_PATH, filename)
        with open(filepath) as f:
            encoded_message_ascii = ''.join(f.readlines())
            encoded_message = base64.b64decode(encoded_message_ascii)

        self.__fmh.process_message(encoded_message)

        replacements = {
            'filepath': filepath,
        }

        new_filepath = _SUCCESS_FILEPATH_TEMPLATE % replacements
        os.rename(filepath, new_filepath)

    def __mark_error(self, filename, message):
        old_filepath = os.path.join(_SPOOL_PATH, filename)
        
        replacements = {
            'filepath': old_filepath,
        }
        
        new_filepath = _FAILED_FILEPATH_TEMPLATE % replacements
        os.rename(old_filepath, new_filepath)

        message_filepath = _FAILED_MESSAGE_FILEPATH_TEMPLATE % replacements

        with open(message_filepath, 'w') as f:
            f.write(message)

    def __thread(self):
        def load_messages():
            return glob.glob(os.path.join(_SPOOL_PATH, _SPOOLED_MESSAGE_PATTERN))

        message_names = load_messages()
        while self.__kill_event.is_set() is False:
            if not message_names:
                message_names = load_messages()

            if message_names:
                _logger.debug("Processing first spooled item.")
                filename = message_names[0]

                try:
                    self.__read_and_process(filename)
                except:
                    _logger.exception("Job failed: [%s]", filename)

                    message = traceback.format_exc()
                    self.__mark_error(filename, message)

                _logger.debug("Removing first spool item.")
                del message_names[0]

                time.sleep(_DELAY_AFTER_JOB_HIT)
            else:
                time.sleep(_DELAY_AFTER_JOB_MISS)

        _logger.warning("Consumer terminating.")


class FakeQueueFactory(mr.queue.queue_factory.QueueFactory):
    def __init__(self, topics):
        self.__producer = _FakeQueueProducer()
        self.__consumer = _FakeQueueConsumer(topics)

        self.__control = _FakeQueueControl(self.__producer, self.__consumer)

    def get_control(self):
        return self.__control

    def get_producer(self):
        return self.__producer

    def get_consumer(self):
        return self.__consumer

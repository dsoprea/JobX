import logging

import mr.queue.queue_message

_logger = logging.getLogger(__name__)


class QueueProducer(object):
    def __init__(self):
        self.__qmf = mr.queue.queue_message.get_queue_message_funnel()
        self.__qmp = mr.queue.queue_message.get_queue_message_processor()

    def is_alive(self):
        """Determine if the client is still connected/healthy."""

        raise NotImplementedError()

    def push_one(self, topic, job_class, data):
        flattened_data = self.__qmf.deflate(data)
        raw_message = self.__qmp.encode(job_class, flattened_data)

        self.push_one_raw(topic, raw_message)

    def push_many(self, topic, job_class, data_list):
        raw_message_list = [self.__qmp.encode(
                                job_class, 
                                self.__qmf.deflate(data)) 
                            for data 
                            in data_list]

        self.push_many_raw(topic, raw_message_list)

    def push_one_raw(self, topic, raw_message):
        """Push one message, already encoded."""

        raise NotImplementedError()

    def push_many_raw(self, topic, raw_message_list):
        """Push a list of messages, each already encoded."""

        raise NotImplementedError()

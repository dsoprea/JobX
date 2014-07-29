import logging

import nsq.consumer
import nsq.node_collection
import nsq.message_handler
#import nsq.identify

import mr.config.nsq_queue
import mr.queue

_logger = logging.getLogger(__name__)


class NsqMessageHandler(
        nsq.message_handler.MessageHandler, 
        mr.queue.MessageHandler):
# TODO(dustin): We need to manage steps, here.
    pass


class _NsqProducerConsumer(
        mr.queue.QueueProducer, 
        mr.queue.QueueConsumer, 
        mr.queue.QueueControl):
    """Interface with NSQ queue, and provides all of the interfaces."""

    def __init__(self, node_collection, context_list, message_handler_cls, 
                 max_in_flight):
        super(mr.queue.QueueProducer, self).__init__()
        super(mr.queue.QueueConsumer, self).__init__()
        super(mr.queue.QueueControl, self).__init__()

        self.__c = nsq.consumer.Consumer(
                    context_list,
                    node_collection, 
                    max_in_flight, 
                    message_handler_cls=message_handler_cls)

    def is_alive(self):
        return self.__c.is_alive

    def start(self):
        self.__c.start()

    def stop(self):
        self.__c.stop()

    def push_one(self, topic, job_class, data):
        self.__c.connection_election.elect_connection().pub(topic, message)

    def push_many(self, topic, job_class, data_list):
        self.__c.connection_election.elect_connection().mpub(topic, messages)


class NsqQueueFactory(mr.queue.QueueFactory):
    def __init__(self):
        context_list = [(mr.config.nsq_queue.TOPIC, 
                         mr.config.nsq_queue.CHANNEL)]

        self.__npc = _NsqProducerConsumer(
                        mr.config.nsq_queue.NODE_COLLECTION, 
                        context_list,
                        NsqMessageHandler,
                        mr.config.nsq_queue.MAX_IN_FLIGHT)

    def get_consumer(self):
        return self.__npc

    def get_producer(self):
        return self.__npc

    def get_control(self):
        return self.__npc

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

        self.__p = mr.queue.get_packager()

    def is_alive(self):
# TODO(dustin): This isn't yet being implemented/facilitated.
        return self.__c.is_alive

    def start(self):
        _logger.info("Starting NSQ.")
        self.__c.start()

    def stop(self):
        _logger.info("Stopping NSQ.")
        self.__c.stop()

    def push_one_raw(self, topic, raw_message):
        _logger.debug("Pushing message to topic: [%s]", topic)

        c = self.__c.connection_election.elect_connection()
        c.pub(topic, raw_message)

    def push_many_raw(self, topic, raw_message_list):
        # We can't indicate -how many- messages there are if we were given a 
        # generator.
        _logger.debug("Pushing MANY messages to topic: [%s]", topic)

        c = self.__c.connection_election.elect_connection()
        c.mpub(topic, raw_message_list)


class NsqQueueFactory(mr.queue.QueueFactory):
    def __init__(self, topics):
        context_list = [(topic, mr.config.nsq_queue.CHANNEL) 
                        for topic 
                        in topics]

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

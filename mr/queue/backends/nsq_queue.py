import logging

import nsq.consumer
import nsq.producer
import nsq.node_collection
import nsq.message_handler
#import nsq.identify

import mr.config.nsq_queue
import mr.queue.queue_factory
import mr.queue.queue_consumer
import mr.queue.queue_producer
import mr.queue.queue_control
import mr.queue.message_handler

logging.getLogger('nsq').setLevel(logging.INFO)

_logger = logging.getLogger(__name__)


class _NsqMessageHandler(nsq.message_handler.MessageHandler):

    def __init__(self, *args, **kwargs):
        nsq.message_handler.MessageHandler.__init__(self, *args, **kwargs)
        self.__mh = mr.queue.message_handler.MessageHandler()

    def classify_message(self, connection, message):
        """Classify the type of message that is about to be processed."""

        # Since we only really care about whether this is a map or reduce 
        # message, and we have different topics for both, we're going to 
        # classify based on the topic, rather than anything in the message 
        # itself.

        topic = connection.context.topic
        (_, workflow_name, class_, capability_name) = topic.split('.')

        # We don't actually need the capability. We would've only received the 
        # message if we declared ourselves to support this capability.

        context = {
            'capability_name': capability_name,
        }

        _logger.debug("Message has class of [%s] and a required-capability of "
                      "[%s].", class_, capability_name)

        return (class_, context)

    def handle_map(self, connection, message, context):
        """We are receiving a map-reduce message (that's all we'll receive). 
        Forward it to standard map-reduce message processing.
        """

        _logger.debug("Received MAP message from queue: [%s]", 
                      message.message_id)

# TODO(dustin): We might want to fork into a new thread.

        try:
            self.__mh.process_message(message.body)
        except:
            # We don't want the message to reappear, later.
            _logger.exception("There was an error while handling the MAP "
                              "message. Squashing it.")
        else:
            _logger.debug("MAP message processed successfully: [%s]", 
                          message.message_id)

    def handle_reduce(self, connection, message, context):
        """We are receiving a map-reduce message (that's all we'll receive). 
        Forward it to standard map-reduce message processing.
        """

        _logger.debug("Received REDUCE message from queue: [%s]", 
                      message.message_id)

# TODO(dustin): We might want to fork into a new thread.

        try:
            self.__mh.process_message(message.body)
        except:
            # We don't want the message to reappear, later.
            _logger.exception("There was an error while handling the REDUCE "
                              "message. Squashing it.")
        else:
            _logger.debug("REDUCE processed successfully: [%s]", 
                          message.message_id)


class _NsqQueueControl(mr.queue.queue_control.QueueControl):
    """Control interface to the NSQ queue."""

    def __init__(self, producer, consumer):
        super(_NsqQueueControl, self).__init__()

        self.__p = producer
        self.__c = consumer

    def start_producer(self):
        _logger.info("Starting NSQ producer.")

        try:
            self.__p.resource.start()
        except:
            _logger.exception("Could not start the queue producer.")
        else:
            return

        raise SystemError("Could not start the queue producer.")

    def start_consumer(self):
        _logger.info("Starting NSQ consumer.")

        try:
            self.__c.resource.start()
        except:
            _logger.exception("Could not start the queue consumer.")
        else:
            return

        raise SystemError("Could not start the queue consumer.")

    def stop_producer(self):
        _logger.info("Stopping NSQ producer.")

        try:
            self.__c.resource.stop()
        except:
            _logger.exception("Could not stop the queue producer.")
        else:
            return

        raise SystemError("Could not stop the queue producer.")

    def stop_consumer(self):
        _logger.info("Stopping NSQ consumer.")

        try:
            self.__c.resource.stop()
        except:
            _logger.exception("Could not stop the queue consumer.")
        else:
            return

        raise SystemError("Could not stop the queue consumer.")


class _NsqQueueProducer(mr.queue.queue_producer.QueueProducer):
    """Producer interface to the NSQ queue."""

    def __init__(self):
        super(_NsqQueueProducer, self).__init__()

        node_collection = mr.config.nsq_queue.NODE_COLLECTION

        self.__p = nsq.producer.Producer(node_collection)

    def is_alive(self):
# TODO(dustin): This isn't yet being implemented/facilitated.
        return self.__p.is_alive

    def push_one_raw(self, topic, raw_message):
        _logger.debug("Pushing message to topic: [%s]", topic)

        c = self.__p.connection_election.elect_connection()
        c.pub(topic, raw_message)

    def push_many_raw(self, topic, raw_message_list):
        # We can't indicate -how many- messages there are if we were given a 
        # generator.
        _logger.debug("Pushing MANY messages to topic: [%s]", topic)

        c = self.__p.connection_election.elect_connection()
        c.mpub(topic, raw_message_list)

    @property
    def resource(self):
        return self.__p


class _NsqQueueConsumer(mr.queue.queue_consumer.QueueConsumer):
    """Consumer interface to the NSQ queue."""

    def __init__(self, topics):
        super(_NsqQueueConsumer, self).__init__()
        node_collection = mr.config.nsq_queue.NODE_COLLECTION

# TODO(dustin): We might have to produce a list of random channels so that 
#               we're properly parallelizing.
        context_list = [(topic, mr.config.nsq_queue.CHANNEL) 
                        for topic 
                        in topics]

        self.__c = nsq.consumer.Consumer(
                context_list,
                node_collection, 
                mr.config.nsq_queue.MAX_IN_FLIGHT, 
                message_handler_cls=_NsqMessageHandler)

    def is_alive(self):
# TODO(dustin): This isn't yet being implemented/facilitated.
        return self.__c.is_alive

    @property
    def resource(self):
        return self.__c


class NsqQueueFactory(mr.queue.queue_factory.QueueFactory):
    def __init__(self, topics):
        self.__producer = _NsqQueueProducer()
        self.__consumer = _NsqQueueConsumer(topics)

        self.__control = _NsqQueueControl(self.__producer, self.__consumer)

    def get_control(self):
        return self.__control

    def get_producer(self):
        return self.__producer

    def get_consumer(self):
        return self.__consumer

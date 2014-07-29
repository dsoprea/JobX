import logging
import pickle
import collections

import mr.config.queue
import mr.utility
import mr.queue_processor
import mr.models.kv.step
import mr.job_engine

QUEUE_INSTANCE_CLS = collections.namedtuple(
                        'QueueInstance', 
                        ['consumer', 'producer', 'control'])

_logger = logging.getLogger(__name__)


class _JobPackager(object):
# TODO(dustin): Consider using protobuf. It's always faster if we know some 
#               structure beforehand.

    def encode(self, job_class, data):
        return pickle.dumps((1, job_class, data))

    def decode(self, encoded):
        (format_version, job_class, data) = pickle.dumps(encoded)
        if format_version != 1:
            raise ValueError("Job format version not valid: (%d)" % 
                             (format_version,))

        return (job_class, data)


packager = _JobPackager()

def get_packager():
    return packager


class MessageHandler(object):
    """Received all data coming off the queue."""

    def __init__(self, *args, **kwargs):
        super(MessageHandler, self).__init__(*args, **kwargs)
        self.__sp = mr.job_engine.get_step_processor()

    def classify_message(self, message):
        return packager.decode(message)

    def handle_map(self, connection, message, context):
        """Corresponds to steps received with a type of ST_MAP."""
# TODO(dustin): Verify that we don't want to convert "message" to something 
#               else, rather than passing directly.
        self.__sp.handle_map(self, message)

    def handle_reduce(self, connection, message, context):
        """Corresponds to steps received with a type of ST_REDUCE."""

# TODO(dustin): Verify that we don't want to convert "message" to something 
#               else, rather than passing directly.
        self.__sp.handle_map(self, message)

    def handle_action(self, connection, message, context):
        """Corresponds to steps received with a type of ST_ACTION."""

# TODO(dustin): Verify that we don't want to convert "message" to something 
#               else, rather than passing directly.
        self.__sp.handle_action(self, message)


class QueueControl(object):
    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class QueueProducer(object):
    def is_alive(self):
        """Determine if the client is still connected/healthy."""

        raise NotImplementedError()

    def push_one(self, topic, job_class, data):
        """Push one message, already encoded."""

        raise NotImplementedError()

    def push_many(self, topic, job_class, data_list):
        """Push a list of messages, each already encoded."""

        raise NotImplementedError()


class QueueConsumer(object):
    def is_alive(self):
        """Determine if the client is still connected/healthy."""

        raise NotImplementedError()

    @property
    def topic(self):
        """What topic are we consuming?"""

        return self.__topic

    @property
    def channel(self):
        """What channel are we consuming on?"""

        return self.__channel


class QueueFactory(object):
    def get_consumer(self):
        raise NotImplementedError()

    def get_producer(self):
        raise NotImplementedError()

    def get_control(self):
        raise NotImplementedError()

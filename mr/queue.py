import logging
import pickle
import collections

import mr.config.queue
import mr.utility

QUEUE_INSTANCE_CLS = collections.namedtuple('QueueInstance', ['consumer', 'producer', 'control'])

_logger = logging.getLogger(__name__)


class _JobPackager(object):
    def encode(self, job_class, data):
        return pickle.dumps((1, job_class, data))

    def decode(self, encoded):
        (format_version, job_class, data) = pickle.dumps(encoded)
        if format_version != 1:
            raise ValueError("Job format version not valid: (%d)" % 
                             (format_version,))

        return (job_class, data)


packager = _JobPackager()


class MessageHandler(object):
    def classify_message(self, message):
        return packager.decode(message)


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
# TODO(dustin): For simplicity under lack of requirement, we only support one 
#               topic and one channel right now. Once we get into it, we might
#               get a clearer idea.
    def __init__(self):
        self.__topic = topic
        self.__channel = channel

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

_QUEUE = None

def boot():
    global _QUEUE

    factory_cls = mr.utility.load_cls_from_string(
                    mr.config.queue.QUEUE_FACTORY_FQ_CLASS)

    factory = queue_factory_cls()

    _QUEUE = QUEUE_INSTANCE_CLS(
                consumer=factory.get_consumer(),
                producer=factory.get_producer(),
                control=factory.get_control())

    _QUEUE.start()

def stop():
    if _QUEUE is not None:
        _QUEUE.stop()

import logging
import pickle
import collections
import functools
import gevent

import mr.config.queue
import mr.models.kv.step
import mr.models.kv.request
import mr.models.kv.job
import mr.models.kv.handler
import mr.models.kv.invocation
import mr.utility
import mr.queue_processor
import mr.job_engine
import mr.workflow_manager
import mr.shared_types

QUEUE_INSTANCE_CLS = collections.namedtuple(
                        'QueueInstance', 
                        ['consumer', 'producer', 'control'])

_QUEUED_DATA_V1_CLS = collections.namedtuple(
                        'QueueMessageV1', 
                        ['workflow_name',
                         'request_id', 
                         'step_name', 
                         'arguments'])

# Queue data format versions.
_QDF_1 = 1

_QUEUE_FORMAT_CLS_MAP = {
    _QDF_1: _QueueDataPackagerV1(),
}

# Set to the current format version.
_CURRENT_QUEUE_FORMAT = _QDF_1

_logger = logging.getLogger(__name__)


class _QueueDataPackager(object):
    def encode(self, data):
        raise NotImplementedError()

    def decode(self, encoded_data):
        raise NotImplementedError()


class _QueueDataPackagerV1(_QueueDataPackager):
    """Encoded/decoded the data going into the messages."""

# TODO(dustin): Consider using protobuf. It's always faster if we know some 
#               structure beforehand.

    def encode(self, data):
        assert issubclass(data.__class__, _QUEUED_DATA_V1_CLS) is True:

        return pickle.dumps(data)

    def decode(self, encoded_data):
        return pickle.loads(encoded_data)

def _get_data_packager(format_version=_CURRENT_QUEUE_FORMAT):
    return _QUEUE_FORMAT_CLS_MAP[format_version]


class _QueueMessagePackager(object):
    """Encodes/decoded the messages going to and from the queue."""

# TODO(dustin): Consider using protobuf. It's always faster if we know some 
#               structure beforehand.

    def encode(self, job_class, data):
        packager = _get_data_packager()

        return pickle.dumps((
                job_class, 
                _CURRENT_QUEUE_FORMAT, 
                packager.encode(data)))

    def decode(self, encoded_message):
        (job_class, format_version, encoded_data) = \
            pickle.loads(encoded_message)

        packager = get_data_packager(format_version)

        return (job_class, format_version, packager.decode(encoded_data))

_qmp = _QueueMessagePackager()


class _QueueMessageFunnel(object):
    """This object knows how to funnel the collection of models that represent 
    a request down to flatter data that comprises the message to be queued, and 
    vice-versa. Note that this is queue-data-format specific, and if we change 
    or improve how stuff is encoded, it needs to be handled here, too. This is
    essentially an adapter for the rest of the application.
    """

    def deflate(self, message_parameters):
        """Return a named-tuple of the data that'll actually be stored in the 
        queue message (this prunes unnecessary data, such as dumping a set of 
            full models in favor of keeping a handful of simple fields).
        """

        assert issubclass(
                message_parameters.__class__, 
                mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS) is True

        workflow = message_parameters.managed_workflow.workflow

        return _QUEUED_DATA_V1_CLS(
                workflow_name=workflow.workflow_name,
                request_id=message_parameters.request.request_id,
                step_name=message_parameters.step.step_name,
                arguments=message_parameters.arguments)

    def inflate(self, format_version, deflated):
        """Reconstruct the battery of models and arguments that describes the 
        original request.
        """

        if format_version == _QDF_1:
            (workflow_name, request_id, step_name, arguments) = deflated

            wm = workflow_manager.get_wm()
            managed_workflow = wm.get(workflow_name)
            workflow = managed_workflow.workflow

            request = mr.models.kv.request.get(workflow, request_id)
            invocation = mr.models.kv.invocation.get(
                            workflow, 
                            request.invocation_id)

            job = mr.models.kv.job.get(workflow, request.job_name)
            step = mr.models.kv.step.get(workflow, step_name)
            handler = mr.models.kv.step.get(workflow, step.handler_name)
        else:
            raise ValueError("Queue data format version is invalid: [%s]" % 
                             (format_version,))

        return mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
                managed_workflow=managed_workflow,
                invocation=invocation,
                request=request,
                job=job,
                step=step,
                handler=handler,
                arguments=arguments)

_qmf = _QueueMessageFunnel()

def get_queue_message_funnel():
    return _qmf


class MessageHandler(object):
    """Received all data coming off the queue."""

    def __init__(self, *args, **kwargs):
        super(MessageHandler, self).__init__(*args, **kwargs)
        self.__sp = mr.job_engine.get_step_processor()

    def classify_message(self, encoded_message):
        (job_class, format_version, decoded_data) = _qmp.decode(encoded_message)
        return (job_class, (format_version, decoded_data))

    def __dispatch(self, handler, decoded_data_info):
        (format_version, decoded_data) = decoded_data_info

        _logger.debug("Dispatching dequeued message: (%d) %s", 
                      format_version, decoded_data)

        qmf = get_queue_message_funnel()
        message_parameters = qmf.inflate(format_version, decoded_data)

# TODO(dustin): Is there a benefit to adding a pool here, for gevent?
        gevent.spawn(handler, message_parameters)

    def handle_map(self, connection, message, context):
        """Corresponds to steps received with a type of ST_MAP."""

        handler = functools.partial(self.__sp.handle_map, connection)
        self.__dispatch(handler, context)

    def handle_reduce(self, connection, message, context):
        """Corresponds to steps received with a type of ST_REDUCE."""

        handler = functools.partial(self.__sp.handle_reduce, connection)
        self.__dispatch(handler, context)


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
        flattened_data = get_queue_message_funnel().deflate(data)
        raw_message = _qmp.encode(job_class, flattened_data)
        self.push_one_raw(topic, raw_message)

    def push_many(self, topic, job_class, data_list):

        # We don't assert like in push_one() because it's not as efficient/
        # elegant here.
        qmf = get_queue_message_funnel()

        raw_message_list = [_qmp.encode(
                                job_class, 
                                qmf.deflate(data)) 
                            for data 
                            in data_list]

        self.push_many_raw(topic, raw_message_list)

    def push_one_raw(self, topic, raw_message):
        """Push one message, already encoded."""

        raise NotImplementedError()

    def push_many_raw(self, topic, raw_message_list):
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

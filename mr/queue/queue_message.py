import logging
import collections
import pickle

import mr.shared_types
import mr.workflow_manager
import mr.models.kv.request
import mr.models.kv.invocation
import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.handler

_logger = logging.getLogger(__name__)

# We have to have a symbol that matches the alleged name in order to pickle.
QueueMessageV1 = collections.namedtuple(
                    'QueueMessageV1', 
                    ['workflow_name',
                     'request_id', 
                     'step_name', 
                     'arguments'])


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
        assert issubclass(data.__class__, QueueMessageV1) is True

        return pickle.dumps(data)

    def decode(self, encoded_data):
        return pickle.loads(encoded_data)

# Queue data format versions.
_QDF_1 = 1

_QUEUE_FORMAT_CLS_MAP = {
    _QDF_1: _QueueDataPackagerV1(),
}

# Set to the current format version.
_CURRENT_QUEUE_FORMAT = _QDF_1

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

        packager = _get_data_packager(format_version)

        return (job_class, format_version, packager.decode(encoded_data))

_qmp = None

def get_queue_message_processor():
    global _qmp

    if _qmp is None:
        _qmp = _QueueMessagePackager()

    return _qmp


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

        workflow = message_parameters.workflow

        # This implements whatever the current message format is.
        return QueueMessageV1(
                workflow_name=workflow.workflow_name,
                request_id=message_parameters.request.request_id,
                step_name=message_parameters.step.step_name,
                arguments=dict(message_parameters.arguments))

    def inflate(self, format_version, deflated):
        """Reconstruct the battery of models and arguments that describes the 
        original request.
        """

        if format_version == _QDF_1:
            (workflow_name, request_id, step_name, arguments) = deflated

            wm = mr.workflow_manager.get_wm()
            managed_workflow = wm.get(workflow_name)
            workflow = managed_workflow.workflow

            request = mr.models.kv.request.get(workflow, request_id)
            invocation = mr.models.kv.invocation.get(
                            workflow, 
                            request.invocation_id)

            job = mr.models.kv.job.get(workflow, request.job_name)
            step = mr.models.kv.step.get(workflow, step_name)
            
            if invocation.direction == mr.constants.D_MAP:
                handler = mr.models.kv.handler.get(workflow, step.map_handler_name)
            elif invocation.direction == mr.constants.D_REDUCE:
                handler = mr.models.kv.handler.get(workflow, step.reduce_handler_name)
            else:
                raise ValueError("Invocation direction [%s] invalid: %s" % 
                                 (invocation.direction, invocation))
        else:
            raise ValueError("Queue data format version is invalid: [%s]" % 
                             (format_version,))

        border = '-' * 79

        _logger.debug("Job has been inflated:\n"
                      "%s\n"
                      "WORKFLOW:\n  %s\n"
                      "INVOCATION:\n  %s\n"
                      "REQUEST:\n  %s\n"
                      "JOB:\n  %s\n"
                      "STEP:\n  %s\n"
                      "HANDLER:\n  %s\n"
                      "ARGUMENTS:\n  %s\n"
                      "%s",
                      border, workflow, invocation, request, job, step, 
                      handler, arguments, border)

        return mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
                workflow=workflow,
                invocation=invocation,
                request=request,
                job=job,
                step=step,
                handler=handler,
                arguments=arguments)

_qmf = _QueueMessageFunnel()

def get_queue_message_funnel():
    return _qmf

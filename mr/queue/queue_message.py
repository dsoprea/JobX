import logging
import collections
import pickle
import time

import mr.config.queue
import mr.shared_types
import mr.workflow_manager
import mr.models.kv.request
import mr.models.kv.invocation
import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.handler
import mr.models.kv.data_layer

_logger = logging.getLogger(__name__)

# We can't prefix with an underscore because we require a symbol that matches 
# the alleged name in order to pickle.

QueueMessageV1 = collections.namedtuple(
                    'QueueMessageV1', 
                    ['workflow_name',
                     'request_id', 
                     'invocation_id',
                     'step_name'])

QueueMessageV2 = collections.namedtuple(
                    'QueueMessageV2', 
                    ['workflow_name',
                     'request_id', 
                     'invocation_id',
                     'step_name',
                     'collective_state'])


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


class _QueueDataPackagerV2(_QueueDataPackager):
    """Encoded/decoded the data going into the messages."""

# TODO(dustin): Consider using protobuf. It's always faster if we know some 
#               structure beforehand.

    def encode(self, data):
        assert issubclass(data.__class__, QueueMessageV2) is True

        return pickle.dumps(data)

    def decode(self, encoded_data):
        return pickle.loads(encoded_data)

## Queue data format versions.

# Initial format.
_QDF_1 = 1

# Updated to store and check model state.
#
# UPDATE: This turned out to be unnecessary (now using *force_consistency* 
#         option to *etcd*). A good solution needs to be handled at the model 
#         level with help of a distributed Redis/memcached KV cache, anyway.
_QDF_2 = 2

_QUEUE_FORMAT_CLS_MAP = {
    _QDF_1: _QueueDataPackagerV1(),
    _QDF_2: _QueueDataPackagerV2(),
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

        if _CURRENT_QUEUE_FORMAT == _QDF_1:
            return QueueMessageV1(
                    workflow_name=workflow.workflow_name,
                    request_id=message_parameters.request.request_id,
                    invocation_id=message_parameters.invocation.invocation_id,
                    step_name=message_parameters.step.step_name)
        elif _CURRENT_QUEUE_FORMAT == _QDF_2:
            sc = mr.models.kv.data_layer.StateCapture()
            sc.set(workflow)
            sc.set(message_parameters.request)
            sc.set(message_parameters.invocation)
            sc.set(message_parameters.job)
            sc.set(message_parameters.step)
            sc.set(message_parameters.handler)

            return QueueMessageV2(
                    workflow_name=workflow.workflow_name,
                    request_id=message_parameters.request.request_id,
                    invocation_id=message_parameters.invocation.invocation_id,
                    step_name=message_parameters.step.step_name,
                    collective_state=sc.get_collective_state())
        else:
            raise ValueError("Queue data format version is invalid for "
                             "deflation: [%s]" % (format_version,))

    def inflate(self, format_version, deflated):
        """Reconstruct the battery of models and arguments that describes the 
        original request.
        """

        def load(workflow_name, request_id, invocation_id, step_name):
            wm = mr.workflow_manager.get_wm()
            managed_workflow = wm.get(workflow_name)
            workflow = managed_workflow.workflow

            request = mr.models.kv.request.get(workflow, request_id)

            invocation = mr.models.kv.invocation.get(
                            workflow, 
                            invocation_id)

            job = mr.models.kv.job.get(workflow, request.job_name)
            step = mr.models.kv.step.get(workflow, step_name)
            
            if invocation.direction == mr.constants.D_MAP:
                handler = mr.models.kv.handler.get(workflow, step.map_handler_name)
            elif invocation.direction == mr.constants.D_REDUCE:
                if step.reduce_handler_name is not None:
                    handler = mr.models.kv.handler.get(workflow, step.reduce_handler_name)
                else:
                    handler = None
            else:
                raise ValueError("Invocation direction [%s] invalid: %s" % 
                                 (invocation.direction, invocation))

            return (workflow, request, invocation, job, step, handler)

        if format_version == _QDF_1:
            (workflow_name, request_id, invocation_id, step_name) = deflated
            r = load(workflow_name, request_id, invocation_id, step_name)
            (workflow, request, invocation, job, step, handler) = r
        elif format_version == _QDF_2:
            (workflow_name, request_id, invocation_id, step_name, collective_state) = deflated

            r = load(workflow_name, request_id, invocation_id, step_name)
            (workflow, request, invocation, job, step, handler) = r

            # Ensure that the recovered models have reached the correct state 
            # (updates have propagated to us). If not, wait until it happens.

            sc = mr.models.kv.data_layer.StateCapture(collective_state)

            faulted_list = None
            checks_remaining = \
                mr.config.queue.MODEL_STATE_PROPAGATION_MAX_CHECKS

            while 1:
                if faulted_list is None:
                    faulted_list = sc.check_states(
                                    workflow, 
                                    request, 
                                    invocation, 
                                    job, 
                                    step, 
                                    handler)
                else:
                    if mr.config.IS_DEBUG is True:
                        for faulted in faulted_list:
                            old_state = int(faulted.state_string)
                            faulted.refresh()
                            new_state = int(faulted.state_string)

                            _logger.debug("%s[%s]: (%d) => (%d) WAITING-ON=(%d)", 
                                          faulted.__class__.__name__, 
                                          str(faulted), old_state, new_state, 
                                          sc.get_desired_state(faulted))

                    faulted_list = sc.check_states(*faulted_list)

                if not faulted_list:
                    _logger.debug("The models described by the message have "
                                  "reached an acceptable state.")
                    break

                if mr.config.IS_DEBUG is True:
                    faulted_phrases = dict([(m.__class__.__name__, str(m)) 
                                            for m 
                                            in faulted_list])

                    _logger.debug("One or more models are not at the latest "
                                  "state. Waiting: %s", faulted_phrases)

                checks_remaining -= 1
                if checks_remaining <= 0:
                    raise IOError("Data has not yet propagated for one or "
                                  "more models, and we've given up: %s" % 
                                  (faulted_list,))

                time.sleep(mr.config.queue.\
                            MODEL_STATE_PROPAGATION_CHECK_FAULT_DELAY_S)
        else:
            raise ValueError("Queue data format version is invalid for "
                             "inflation: [%s]" % (format_version,))

        border = '-' * 79

        _logger.debug("Message has been inflated:\n"
                      "%s\n"
                      "WORKFLOW:\n  %s\n"
                      "INVOCATION:\n  %s\n"
                      "REQUEST:\n  %s\n"
                      "JOB:\n  %s\n"
                      "STEP:\n  %s\n"
                      "HANDLER:\n  %s\n"
                      "%s",
                      border, workflow, invocation, request, job, step, 
                      handler, border)

        return mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
                workflow=workflow,
                invocation=invocation,
                request=request,
                job=job,
                step=step,
                handler=handler)

_qmf = _QueueMessageFunnel()

def get_queue_message_funnel():
    return _qmf

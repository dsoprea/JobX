import os
import logging

import mr.constants

_logger = logging.getLogger(__name__)

_USE_FAKE_QUEUE = bool(int(os.environ.get('MR_USE_FAKE_QUEUE', '0')))

if _USE_FAKE_QUEUE is True:
    _logger.warning("'Fake' queue elected.")
    QUEUE_FACTORY_FQ_CLASS = 'mr.queue.backends.fake_queue.FakeQueueFactory'
else:
    QUEUE_FACTORY_FQ_CLASS = 'mr.queue.backends.nsq_queue.NsqQueueFactory'

IS_MULTITHREADED = bool(int(os.environ.get('MR_MULTITHREADED', '1')))

TOPIC_NAME_MAP_TEMPLATE = 'mr.%(workflow_name)s.map.%(capability_name)s'
TOPIC_NAME_REDUCE_TEMPLATE = 'mr.%(workflow_name)s.reduce.%(capability_name)s'

def get_current_workflows():
    """Lazy-load the workflow name(s). this won't [probably] be necessary unless 
    we're actually running job-processing, so we don't want to throw an error 
    when we happen to be loaded under other functions.
    """

    default_workflow = 'test_workflow'
    WORKFLOWS_RAW = os.environ.get('MR_WORKFLOW_NAMES', default_workflow)
    return WORKFLOWS_RAW.split(',')

CONSUMER_ENABLED = bool(int(os.environ.get('MR_CONSUME', '1')))

DISPATCH_CLEANUP_INTERVAL_S = 1

_CAPABILITIES = os.environ.get(
                    'MR_SYSTEM_CAPABILITIES', 
                    mr.constants.CAP_GENERAL)

LOCAL_SYSTEM_CAPABILITIES = _CAPABILITIES.split(',')

MODEL_STATE_PROPAGATION_CHECK_FAULT_DELAY_S = .3
MODEL_STATE_PROPAGATION_MAX_CHECKS = 10

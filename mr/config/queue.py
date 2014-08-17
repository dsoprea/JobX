import os

QUEUE_FACTORY_FQ_CLASS = 'mr.queue.nsq_queue.NsqQueueFactory'

TOPIC_NAME_MAP_TEMPLATE = 'mr.%(workflow_name)s.map'
TOPIC_NAME_REDUCE_TEMPLATE = 'mr.%(workflow_name)s.reduce'

def get_current_workflows():
    """Lazy-load the workflow name(s). this won't [probably] be necessary unless 
    we're actually running job-processing, so we don't want to throw an error 
    when we happen to be loaded under other functions.
    """

    return os.environ['MR_WORKFLOW_NAMES'].split(',')

CONSUMER_ENABLED = bool(int(os.environ.get('MR_CONSUME', '1')))

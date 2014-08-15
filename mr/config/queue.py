QUEUE_FACTORY_FQ_CLASS = 'mr.nsq_queue.NsqQueueFactory'

TOPIC_NAME_MAP_TEMPLATE = 'mr.%(workflow_name)s.map'
TOPIC_NAME_REDUCE_TEMPLATE = 'mr.%(workflow_name)s.reduce'

WORKFLOW_NAME_ENVIRONMENT_VARIABLE_NAME = 'MR_WORKFLOW_NAME'

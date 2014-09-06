import collections

QUEUE_MESSAGE_PARAMETERS_CLS = collections.namedtuple(
                                    'InflatedModels', 
                                    ['workflow', 
                                     'invocation',
                                     'request', 
                                     'job', 
                                     'step',
                                     'handler'])#,
                                     #'arguments'])

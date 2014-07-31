import collections

QUEUE_MESSAGE_PARAMETERS_CLS = collections.namedtuple(
                                    'InflatedModels', 
                                    ['managed_workflow', 
                                     'invocation',
                                     'request', 
                                     'job', 
                                     'step',
                                     'handler',
                                     'arguments'])

import collections

QUEUE_INSTANCE_CLS = collections.namedtuple(
                        'QueueInstance', 
                        ['consumer', 'producer', 'control'])

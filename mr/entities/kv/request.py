import collections

REQUEST_CLS = collections.namedtuple(
                'Request', 
                ('workflow_name',
                 'job_name', 
                 'arguments'))

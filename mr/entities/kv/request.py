import collections

REQUEST_CLS = collections.namedtuple(
                'Request', 
                ('request_id',
                 'workflow_name',
                 'job_name', 
                 'arguments'))

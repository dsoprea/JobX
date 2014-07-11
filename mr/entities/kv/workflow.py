import collections

WORKFLOW_CLS = collections.namedtuple(
                'Workflow', 
                ('description'))

INVOKED_WORKFLOW_CLS = collections.namedtuple(
                        'InvokedWorkflow', 
                        ('id', 'workflow_name', 'job_name'))

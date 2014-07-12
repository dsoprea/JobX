import collections

WORKFLOW_CLS = collections.namedtuple(
                'Workflow', 
                ('workflow_name',
                 'description'))

INVOKED_WORKFLOW_CLS = collections.namedtuple(
                        'InvokedWorkflow', 
                        ('invoked_workflow_id', 
                         'workflow_name', 
                         'job_name',
                         'context'))

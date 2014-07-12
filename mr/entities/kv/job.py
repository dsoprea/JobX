import collections

JOB_CLS = collections.namedtuple(
            'Job', 
            ('job_name',
             'workflow_name',
             'description',
             'initial_step_name'))

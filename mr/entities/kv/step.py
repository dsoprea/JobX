import collections

STEP_CLS = collections.namedtuple(
            'Step', 
            ('workflow_name',
             'name',
             'description',
             'argument_keys',
             'code_version',
             'code_type',
             'code_body'))

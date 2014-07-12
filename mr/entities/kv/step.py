import collections

STEP_CLS = collections.namedtuple(
            'Step', 
            ('workflow_name',
             'step_name',
             'description',
             'argument_keys',
             'code_hash',
             'code_type',
             'code_body'))

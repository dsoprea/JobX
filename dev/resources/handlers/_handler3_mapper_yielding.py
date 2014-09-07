"""
mapper test handler 3 (with yields)

**
argument_spec:
    -
        name: arg1
        type: int

handler_type: mapper
"""

import math

yield 'step1'

yielded_step_args = {'arg1': math.sqrt(arg1), 'arg2': 10.0}

yield yielded_step_args

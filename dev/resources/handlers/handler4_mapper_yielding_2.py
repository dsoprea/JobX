"""
Yields many downstream steps

**
argument_spec:
    -
        name: arg1
        type: int

handler_type: mapper
"""

import random

yield 'step5'

count = random.randrange(1, arg1)

while count > 1:
    interval = random.randrange(1, count)
    count -= interval

    yielded_step_args = { 'arg1': interval }

    yield yielded_step_args

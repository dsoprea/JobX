"""
Yields many downstream steps

**
argument_spec:
    -
        name: arguments
        type: list

handler_type: mapper
required_capability: none
"""

import random

arg_dict = dict(arguments)

#yield MrConfigureToMap('step5')
yield MrConfigureToReturn()

count = random.randrange(1, arg_dict['arg1'])

while count > 1:
    interval = random.randrange(1, count)
    count -= interval

    #yield ('arg1', interval)
    yield (random.randrange(10), interval)

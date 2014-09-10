"""
Yields a set of values. Since we're mapped-to, we expect a list of pairs of 
keys and value-lists. We'll only operate on the first value in each value-list,
though.

**
argument_spec:
    -
        name: arguments
        type: list

handler_type: mapper
"""

import random

# we're 

pair = arguments[0]
value = pair[1]

yield MrConfigureToReturn()

count = random.randrange(1, value)

while count > 1:
    interval = random.randrange(1, count)
    count -= interval

    yield (random.randrange(10), interval)

"""
reducer test handler 2

**
argument_spec: 
    - 
        name: results
        type: list

handler_type: reducer
required_capability: none
"""

print("handler2 results: %s" % (results,))

tally = {}
for k, value_list in results:
    sum_ = sum(value_list)

    try:
        tally[k] += sum_
    except KeyError:
        tally[k] = sum_

return tally.items()

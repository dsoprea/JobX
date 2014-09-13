"""
multiply reducer

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
    for value in value_list:
        try:
            tally[k] *= value 
        except KeyError:
            tally[k] = value

return tally.items()

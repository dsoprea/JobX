"""
A reducer that just forwards what it receives.

**
argument_spec: 
    - 
        name: results
        type: list

handler_type: reducer
required_capability: none
"""

for k, v in results:
    yield (k, v)

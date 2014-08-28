"""
reducer test handler 2

**
argument_spec: 
    - 
        name: results
        type: list

handler_type: reducer
"""

return sum([result['datum'] for result in results])

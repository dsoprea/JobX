"""
mapper test handler 1

**
argument_spec: 
    -
        name: arg1
        type: int
    -
        name: arg2
        type: int

handler_type: mapper
"""

yield ('', arg1 + arg2)

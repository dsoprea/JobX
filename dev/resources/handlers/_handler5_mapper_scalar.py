"""
Just return a scalar

**
argument_spec:
    -
        name: arguments
        type: list

handler_type: mapper
"""

arguments = dict(arguments)

return [('datum', sum(arguments['arg1']))]

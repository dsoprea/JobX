"""
Split an argument in half, and send to a downstream step.

**
argument_spec:
    -
        name: arguments
        type: list

handler_type: mapper
"""

arg_dict = dict(arguments)

yield MrConfigureToMap('step7_chunk_argument')

arg = arg_dict['arg1']
half = arg // 2

yield (0, half)
yield (1, half + (arg % 2))

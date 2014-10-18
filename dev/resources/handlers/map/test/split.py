"""
Split an argument in half, and send to a downstream step.

**
argument_spec:
    -
        name: arguments
        type: list

handler_type: mapper
required_capability: none
"""

arg_dict = dict(arguments)

#NOTIFY_EMAIL.info("Test message.")

LOG.info("mapper(split): Top.")

print("SET(map) 1")
ctx.session_set('key1', 'xyz')
print("SET(map) 2")
ctx.session_set('key2', 'uvw')
print("GET(map): %s" % (ctx.session_get('key2'),))

yield MrConfigureToMap('step7_chunk_argument')

arg = arg_dict['arg1']
half = arg // 2

yield (0, half)
yield (1, half + (arg % 2))

LOG.info("mapper(split): Split arguments into two sets.")

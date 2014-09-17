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
required_capability: none
"""

import logging
import random
import pprint

_logger = logging.getLogger(__name__)

_PATH = 'tempdir'

if FS.exists(_PATH) is False:
    FS.mkdir(_PATH)
# TODO(dustin): It seems like we request to delete an entry, and two or three 
#               entries later we get an error about removing the earlier entry. 
#               There might be some statefulness that we're not correctly 
#               accounting for (maybe complicated by the generator).
#
#else:
#    print("CLEANING.")
#
#    filenames = [filename for (filename, stat) in FS.ls(_PATH)]
#
#    pprint.pprint(filenames)
#
#    for filename in filenames:
#        _logger.info("Removing old work file: [%s]", filename)
#        FS.rm(join(_PATH, filename))

_FILEPATH = _PATH + SEP + str(random.random())

print("NEW FILE: [%s]" % (_FILEPATH,))

with FS.open(_FILEPATH, 'w') as f:
    f.write("Body!")

# We're only using the value in the first pair.

pair = arguments[0]
value = pair[1]

yield MrConfigureToReturn()

count = random.randrange(1, value)

while count > 1:
    interval = random.randrange(1, count)
    count -= interval

    yield (random.randrange(10), interval)

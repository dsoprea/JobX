import os

import mr.constants

SOURCE_EXTENSION = 'py'
SOURCE_META_FILENAME_SUFFIX = '.py.meta'
REQUIRED_META_FIELDS = [
    'argument_spec',
    'handler_type',
    'required_capability',
]

CODE_EXTENSION_MAP = {
    'py': mr.constants.CODE_PYTHON,
}

CODE_PROCESSOR_MAP = {
    mr.constants.CODE_PYTHON: 'mr.handlers.processors.python.PythonProcessor',
}

# Define the handler-scope factory. This should return a function given a 
# workflow name. The function should return a dictionary, given a handler name.
# The dictionary will be merged into the global scope of the handler.
#
# This is a great way to equip your handlers with database access and such.

try:
    WORKFLOW_SCOPE_FACTORY_FQ_CLASS = \
        os.environ['MR_WORKFLOW_SCOPE_FACTORY_FQ_CLASS']
except KeyError:
    WORKFLOW_SCOPE_FACTORY_FQ_CLASS = None

HANDLER_UPDATE_INTERVAL_S = int(os.environ.get(
                                    'MR_HANDLER_UPDATE_CHECK_INTERVAL_S', 
                                    '10'))

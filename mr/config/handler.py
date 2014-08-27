import mr.constants

SOURCE_EXTENSION = 'py'
SOURCE_META_FILENAME_SUFFIX = '.py.meta'
REQUIRED_META_FIELDS = [
    'argument_spec',
    'handler_type',
]

CODE_EXTENSION_MAP = {
    'py': mr.constants.CODE_PYTHON,
}

CODE_PROCESSOR_MAP = {
    mr.constants.CODE_PYTHON: 'mr.handlers.processors.python.PythonProcessor',
}

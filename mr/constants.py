# Identity classes.

ID_WORKFLOW = 'workflow'
ID_JOB = 'job'
ID_REQUEST = 'request'
ID_STEP = 'step'
ID_HANDLER = 'handler'
ID_INVOCATION = 'invocation'

# Code types.

CODE_PYTHON = 'python'

CODE_TYPES = (CODE_PYTHON,)

# Flow direction (map/downstream, reduce/upstream).

D_MAP = 'map'
D_REDUCE = 'reduce'

DIRECTIONS = (D_MAP, D_REDUCE)

# Capabilities

CAP_GENERAL = 'general'
REQUIRED_CAP_NONE = 'none'

# Image types.

IT_PNG = 'png'

# Cache types.

CT_REDIS = 'redis'

# Timestamp formats.

DATETIME_STD = '%Y-%m-%d %H:%M:%S'

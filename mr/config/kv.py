import json

ENTITY_ROOT = ('entities',)
ENTITY_TREE_ROOT = ('entity_trees',)

REPR_DATA_TRUNCATE_WIDTH = 20

DEFAULT_ATOMIC_UPDATE_MAX_ATTEMPTS = 5

ENCODER = lambda data: json.dumps(data)
DECODER = lambda encoded: json.loads(encoded)

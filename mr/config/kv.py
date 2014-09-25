import os
import json

ENTITY_ROOT = ('entities',)
ENTITY_TREE_ROOT = ('entity_trees',)
QUEUE_ROOT = ('queues',)

REPR_DATA_TRUNCATE_WIDTH = 20

DEFAULT_ATOMIC_UPDATE_MAX_ATTEMPTS = 5

ENCODER = json.dumps
DECODER = json.loads

IS_CACHED = bool(int(os.environ.get('MR_KV_CACHE', '0')))

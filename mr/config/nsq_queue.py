import os

import nsq.node_collection

TOPIC = os.environ.get('MR_TOPIC', 'general_topic')
CHANNEL = os.environ.get('MR_CHANNEL', 'general_worker')

_LOOKUP_NODES = os.environ.get('MR_NSQLOOKUPD_HOSTS', '').split(',')
_SERVER_NODES = os.environ.get('MR_NSQD_HOSTS', '').split(',')

if _LOOKUP_NODES and _SERVER_NODES:
    raise EnvironmentError("Please provide either lookup nodes -or- server nodes.")

if not _LOOKUP_NODES and not _SERVER_NODES:
    _LOOKUP_NODES = ['http://127.0.0.1:4161']
    _SERVER_NODES = None

if not _LOOKUP_NODES:
    _LOOKUP_NODES = None

if not _SERVER_NODES:
    _SERVER_NODES = None

if _LOOKUP_NODES is not None:
    NODE_COLLECTION = nsq.node_collection.LookupNodes(_LOOKUP_NODES)
else:
    NODE_COLLECTION = nsq.node_collection.ServerNodes(_SERVER_NODES)

MAX_IN_FLIGHT = int(os.environ.get('MR_NSQ_MAX_IN_FLIGHT', '20000'))

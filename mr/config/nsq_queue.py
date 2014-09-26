import os

import nsq.node_collection

CHANNEL = os.environ.get('MR_CHANNEL', 'general_worker')

# We have to be both a producer and a consumer, and a producer can't use 
# lookup-hosts. Therefore, we use the same nsqd server-list for both the 
# producer and the consumer.

_SERVER_HOSTS_PHRASE = os.environ.get('MR_NSQD_HOSTS', '')
_SERVER_NODES_COUPLETS = _SERVER_HOSTS_PHRASE.split(',') if _SERVER_HOSTS_PHRASE else []
_SERVER_NODES = [tuple(c.split(':')) for c in _SERVER_NODES_COUPLETS]

if not _SERVER_NODES:
    _SERVER_NODES = [('127.0.0.1', 4150)]

NODE_COLLECTION = nsq.node_collection.ServerNodes(_SERVER_NODES)

MAX_IN_FLIGHT = int(os.environ.get('MR_NSQ_MAX_IN_FLIGHT', '20000'))

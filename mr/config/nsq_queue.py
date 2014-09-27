import os

import nsq.node_collection

CHANNEL = os.environ.get('MR_CHANNEL', 'general_worker')

_DEFAULT_PORT = 4150

# We have to be both a producer and a consumer, and a producer can't use 
# lookup-hosts. Therefore, we use the same nsqd server-list for both the 
# producer and the consumer.

_SERVER_HOSTS_PHRASE = os.environ.get('MR_NSQD_HOSTS', '')
_SERVER_NODES_COUPLETS = _SERVER_HOSTS_PHRASE.split(',') \
                            if _SERVER_HOSTS_PHRASE \
                            else []

# Split the list of servers out of the environment, and allow the port to be 
# optional.

_SERVER_NODES = []
for c in _SERVER_NODES_COUPLETS:
    parts = c.split(':')
    if len(parts) == 1:
        parts.append(_DEFAULT_PORT)

    _SERVER_NODES.append(tuple(parts))

if not _SERVER_NODES:
    _SERVER_NODES = [('127.0.0.1', _DEFAULT_PORT)]

NODE_COLLECTION = nsq.node_collection.ServerNodes(_SERVER_NODES)

MAX_IN_FLIGHT = int(os.environ.get('MR_NSQ_MAX_IN_FLIGHT', '20000'))

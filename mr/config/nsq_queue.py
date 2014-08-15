import os

import nsq.node_collection

CHANNEL = os.environ.get('MR_CHANNEL', 'general_worker')

# TODO(dustin): Realistically, we have to be both a producer and a consumer, 
#               and a producer can't use lookup-codes. Therefore, we use the 
#               same nsqd server-list for both the producer and the consumer.

#_LOOKUP_HOSTS_PHRASE = os.environ.get('MR_NSQLOOKUPD_HOSTS', '')
#_LOOKUP_NODES = _LOOKUP_HOSTS_PHRASE.split(',') if _LOOKUP_HOSTS_PHRASE else []
_LOOKUP_NODES = []

_SERVER_HOSTS_PHRASE = os.environ.get('MR_NSQD_HOSTS', '')
_SERVER_NODES = _SERVER_HOSTS_PHRASE.split(',') if _SERVER_HOSTS_PHRASE else []

if _LOOKUP_NODES and _SERVER_NODES:
    raise EnvironmentError("Please provide either lookup nodes -or- server nodes.")

if not _LOOKUP_NODES and not _SERVER_NODES:
#    _LOOKUP_NODES = ['http://127.0.0.1:4161']
#    _SERVER_NODES = None
    _SERVER_NODES = [('127.0.0.1', 4150)]

if not _LOOKUP_NODES:
    _LOOKUP_NODES = None

if not _SERVER_NODES:
    _SERVER_NODES = None

if _LOOKUP_NODES is not None:
    NODE_COLLECTION = nsq.node_collection.LookupNodes(_LOOKUP_NODES)
else:
    NODE_COLLECTION = nsq.node_collection.ServerNodes(_SERVER_NODES)

MAX_IN_FLIGHT = int(os.environ.get('MR_NSQ_MAX_IN_FLIGHT', '20000'))

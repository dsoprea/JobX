import logging
import sys
import os

os.environ['ETCD_GEVENT'] = '1'

import etcd.client
import etcd.exceptions

import mr.config
import mr.config.etcd
import mr.config.cache
import mr.config.kv
import mr.models.kv.common

logging.getLogger('etcd').setLevel(logging.INFO)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

_etcd = etcd.client.Client(**mr.config.etcd.CLIENT_CONFIG)


class KvException(Exception):
    pass


class KvPreconditionException(KvException):
    pass


class KvWaitFaultException(KvException):
    pass


class StateCapture(object):
    """This class allows us to persist the current state of a set of models, 
    and to later make sure that a set of models has reached or exceeded the 
    original state. This allows us to be able to wait for propagation of KV 
    updates across servers.

    This requires some *etcd*-specific knowledge, whereby we know that states 
    are monotonically-incrementing integers used for versioning. If we ever
    change backends, this will have to be updated.
    """

    def __init__(self, states={}):
        self.__states = states

    def set(self, model):
        assert model.state_string

        self.__states[self.__get_key(model)] = int(model.state_string)

    def get_collective_state(self):
        return self.__states

    def __get_key(self, model):
        return (model.__class__.__name__, model.get_key())

    def get_desired_state(self, model):
        return self.__states[self.__get_key(model)]

    def check_states(self, *models):
        """Return a list of the models that have still not reached the current 
        state.
        """
# TODO(dustin): We should just short-circuit and check the latest state-string 
#               of the models against the latest state-string in the 
#               dictionary, and include a list of the models that are supposed 
#               to have state-strings greater than the latest state-string of 
#               the models we've been given.
        faulted = []
        for model in models:
            desired_state = self.__states[self.__get_key(model)]
            current_state = int(model.state_string)

            if mr.config.IS_DEBUG is True:
                _logger.debug("%s(%s): Checking model desired state (%d) "
                              "against current state (%d).", 
                              model.__class__.__name__, str(model), 
                              desired_state, current_state)

            if desired_state > current_state:
                _logger.debug("%s(%s): Model changes have not yet propagated.", 
                              model.__class__.__name__, str(model))

                faulted.append(model)

        return faulted


class DataLayerKv(mr.models.kv.common.CommonKv):
    def __init__(self, *args, **kwargs):
        super(DataLayerKv, self).__init__(*args, **kwargs)
        
        module = sys.modules[__name__]

# TODO(dustin): Finish implementing KV-cache layer.
#        if mr.config.kv.IS_CACHED is True:
#            try:
#                cache = getattr(module, '_cache')
#            except AttributeError:
#                cache = mr.config.cache.CACHE_CLS()
#                setattr(module, '_cache', cache)

    def get(self, identity, wait_for_state=None):
        key = self.__class__.flatten_identity(identity)

        # We ignore the wait_for_state parameter because we'll always get the 
        # most-current result.
        response = _etcd.node.get(key, force_consistent=True)

        return (
            response.node.modified_index,
            response.node.value
        )

    def exists(self, identity, wait_for_state=None):
        key = self.__class__.flatten_identity(identity)
        
        try:
            # We ignore the wait_for_state parameter because we'll always get 
            # the most-current result.
            _etcd.node.get(key, force_consistent=True)
        except KeyError:
            return False
        else:
            return True

    def set(self, identity, encoded_data):
        key = self.__class__.flatten_identity(identity)
        return _etcd.node.set(key, encoded_data)

    def update_only(self, identity, encoded_data, check_against_state=None):
        key = self.__class__.flatten_identity(identity)

        try:
            return _etcd.node.compare_and_swap(
                        key, 
                        encoded_data, 
                        prev_exists=True, 
                        current_index=check_against_state)
        except etcd.exceptions.EtcdPreconditionException:
            pass

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise KvPreconditionException("Update or state precondition failed.")

    def create_only(self, identity, encoded_data):
        key = self.__class__.flatten_identity(identity)

        try:
            response = _etcd.node.create_only(key, encoded_data)
        except etcd.exceptions.EtcdPreconditionException:
            pass
        else:
            return response.node.created_index

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise KvPreconditionException("Create or state precondition failed.")

    def delete(self, identity):
        key = self.__class__.flatten_identity(identity)
        return _etcd.node.delete(key)

    def directory_create_only(self, identity):
        key = self.__class__.flatten_identity(identity)
        return _etcd.directory.create(key)

    def directory_delete(self, identity):
        key = self.__class__.flatten_identity(identity)
        return _etcd.directory.delete_recursive(key)

    def directory_exists(self, identity):
        key = self.__class__.flatten_identity(identity)
        _etcd.node.get(key)

    def list(self, root_identity):
        root_key = self.__class__.flatten_identity(root_identity)

        response = _etcd.node.get(root_key)
        for child in response.node.children:
            # Derive the identity for this child as well (clip the search-key 
            # path-prefix from the child-key).
            yield (child.key[len(root_key) + 1:], child.value)

    def list_keys(self, root_identity):
        root_key = self.__class__.flatten_identity(root_identity)

# TODO(dustin): Currently, we have to receive the compete response from *etcd* 
#               before we can operate on it. That response also includes 
#               values, which we don't need at all right here. This needs to be 
#               an improvement within *etcd*.

        response = _etcd.node.get(root_key)
        for child in response.node.children:
            yield child.key[len(root_key) + 1:]

    def wait(self, identity):
        """Wait for a change to exactly one node (not recursive)."""

        key = self.__class__.flatten_identity(identity)
        
        try:
            response = _etcd.node.wait(key, force_consistent=True)
        except etcd.exceptions.EtcdWaitFaultException:
            pass
        else:
            return (
                response.node.modified_index,
                response.node.value
            )

        raise KvWaitFaultException()

    def directory_wait(self, identity, recursive=True):
        """Wait for a change to exactly one node (not recursive)."""

        key = self.__class__.flatten_identity(identity)
        
        try:
            response = _etcd.directory.wait(
                        key, 
                        recursive=recursive, 
                        force_consistent=True)
        except etcd.exceptions.EtcdWaitFaultException:
            pass
        else:
            return (
                response.node.modified_index,
            )

        raise KvWaitFaultException()

# TODO(dustin): Test and replace our existing implementation with this.
    def atomic_update(self, identity, update_value_cb):
        key = self.__class__.flatten_identity(identity)
        response = etcd.node.atomic_update(key, update_value_cb)

        return (
            response.node.modified_index,
            response.node.value
        )


class QueueLayerKv(mr.models.kv.common.CommonKv):
    """Model a queue on top of the KV, where the queue guarantees that you will 
    enumerate the children in the order that they were added. As we expect the 
    KV to act more like persistent storage than a queue, we expect that the 
    members will allow random/selective access, and not be removed until 
    explicitly desired. Therefore, the only real difference between a queue and 
    a normal directory, is the guaranteed ordering in the return.
    """

    def __init__(self, root_identity):
        cls = self.__class__

        self.__root_identity = root_identity
        root_key = cls.flatten_identity(self.__root_identity)

        _logger.debug("Queue resource created: [%s] => [%s]",
                      root_identity, root_key)

        self.__io = _etcd.inorder.get_inorder(root_key)

        self.__dl = DataLayerKv()

    @property
    def root_identity(self):
        return self.__root_identity

    def create(self):
        self.__io.create()

    def exists(self, wait_for_state=None):
        return self.__dl.exists(
                self.__root_identity, 
                wait_for_state=wait_for_state)

    def add(self, encoded_data):
        """This should return an ID for the queued item, so it can be recalled 
        later.
        """

        r = self.__io.add(encoded_data)

        # With *etcd*, the name of the queued item is its modified-index.
        return str(r.node.modified_index)

    def update(self, key, encoded_data):
        self.__dl.update_only(
            self.__root_identity + (key,), 
            encoded_data)

    def get(self, key):
        return self.__dl.get(self.__root_identity + (key,))

    def list(self):
        return self.__dl.list(self.__root_identity)

    def list_keys(self):
        return self.__dl.list_keys(self.__root_identity)

    def list_data(self):
        return (d for (k, d) in self.__dl.list(self.__root_identity))

    def delete_key(self, key):
        self.__io.pop(key)

    def delete(self):
        self.__io.delete()

    def wait_for_change(self):
        """Wait for a change to exactly one node (not recursive)."""

        try:
            return self.__dl.directory_wait(
                    self.__root_identity)
        except etcd.exceptions.EtcdWaitFaultException, \
               etcd.exceptions.EtcdEmptyResponseError:
            raise KvWaitFaultException()

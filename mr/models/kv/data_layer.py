import logging

import etcd.client
import etcd.exceptions

import mr.config.etcd
import mr.models.kv.common

logging.getLogger('etcd').setLevel(logging.INFO)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

_etcd = etcd.client.Client(**mr.config.etcd.CLIENT_CONFIG)


class KvPreconditionException(Exception):
    pass


class DataLayerKv(mr.models.kv.common.CommonKv):
    def get(self, identity, wait_for_state=None):
# TODO(dustin): Finish implementing wait_for_state. We might need to rename 
#               from "state" to "version" so that we can admit that it's an 
#               integer while still staying a bit decoupled from etcd.
        key = self.__class__.flatten_identity(identity)
        response = _etcd.node.get(key)

        return (
            response.node.modified_index,
            response.node.value
        )

    def update_only(self, identity, value, check_against_state=None):
        key = self.__class__.flatten_identity(identity)

        try:
            return _etcd.node.compare_and_swap(
                        key, 
                        value, 
                        prev_exists=True, 
                        current_index=check_against_state)
        except etcd.exceptions.EtcdPreconditionException:
            pass

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise KvPreconditionException("Update or state precondition failed.")

    def create_only(self, identity, value):
        key = self.__class__.flatten_identity(identity)

        try:
            return _etcd.node.create_only(key, value)
        except etcd.exceptions.EtcdPreconditionException:
            pass

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

    def list(self, root_identity):
        root_key = self.__class__.flatten_identity(root_identity)

        response = _etcd.node.get(root_key)
        for child in response.node.children:
            # Don't just decode the data, but derive the identity for this 
            # child as well (clip the search-key path-prefix from the child-key).
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

# TODO(dustin): We need to call node- and directory-specific wait methods.
    def wait_for_node_change(self, identity, recursive=True):
        """Wait for a change to exactly one node (not recursive)."""

        key = self.__class__.flatten_identity(identity)
        response = _etcd.node.wait(key, recursive=recursive)

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
        self.__dl.directory_create_only(self.__root_identity)

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
        return self.__dl.delete(self.__root_identity + (key,))

    def delete(self):
        return self.__dl.directory_delete(self.__root_identity)

    def wait_for_change(self):
        """Wait for a change to exactly one node (not recursive)."""

        return self.__dl.wait_for_node_change(self.__root_identity)

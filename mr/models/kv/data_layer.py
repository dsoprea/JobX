import logging

import etcd.client
import etcd.exceptions

import mr.config.etcd
import mr.models.kv.common
import mr.compat

logging.getLogger('etcd').setLevel(logging.INFO)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

_etcd = etcd.client.Client(**mr.config.etcd.CLIENT_CONFIG)


class DataLayerKv(mr.models.kv.common.CommonKv):
    def get(self, identity):
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
        raise ValueError("Key to update doesn't already exist: [%s]" % (key))

    def create_only(self, identity, value):
        key = self.__class__.flatten_identity(identity)

        try:
            return _etcd.node.create_only(key, value)
        except etcd.exceptions.EtcdPreconditionException:
            pass

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise ValueError("Key to create already exists: [%s]" % (key))

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

    def wait_for_node_change(self, identity):
        """Wait for a change to exactly one node (not recursive)."""

        key = self.__class__.flatten_identity(identity)
        response = _etcd.node.wait(key)

        return (
            response.node.modified_index,
            response.node.value
        )

    @classmethod
    def flatten_identity(cls, identity):
        """Derive a key from the identity string. It can either be a flat 
        string or a tuple of flat-strings. The latter will be collapsed to a
        path name (for heirarchical storage). We may or may not do something
        with hyphens in the future.
        """

        if issubclass(identity.__class__, tuple) is True:
            for part in identity:
                if issubclass(part.__class__, mr.compat.basestring) and \
                   ('-' in part or '/' in part):
                    raise ValueError("Identity has reserved characters: [%s]" % 
                                     (identity,))

            key = '/' + '/'.join([str(part) for part in identity])
        else:
            if issubclass(part.__class__, mr.compat.basestring) and \
               ('-' in identity or '/' in identity):
                raise ValueError("Identity has reserved characters: [%s]" % 
                                 (identity,))

            key = identity

        _logger.debug("Flattening identity: [%s] => [%s]", identity, key)

        return key

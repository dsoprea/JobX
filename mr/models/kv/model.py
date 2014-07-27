import logging
import json
import random
import hashlib

import etcd.client
import etcd.exceptions

import mr.config.etcd

logging.getLogger('etcd').setLevel(logging.INFO)

_ENTITY_ROOT = 'entities'

_logger = logging.getLogger(__name__)

_encode = lambda data: json.dumps(data)
_decode = lambda encoded: json.loads(encoded)

_etcd = etcd.client.Client(**mr.config.etcd.CLIENT_CONFIG)


class Field(str):
    pass


class Model(object):
    entity_class = None

    def __init__(self, *args, **fields):
        expected_fields = set(self.__get_field_names())
        actual_fields = set(fields)

        assert actual_fields == expected_fields, \
               "Fields given [%s] do not match fields expected [%s]." % \
               (actual_fields, expected_fields)

        for k, v in fields.items():
            setattr(self, k, v)

    def __get_field_names(self):
        # We use None so we don't error with the private attributes.
        comparator = lambda k: issubclass(
                                getattr(self.__class__, k, None).__class__, 
                                Field)

        return filter(comparator, dir(self))

    def get_data(self):
        return dict([(k, getattr(self, k)) for k in self.__get_field_names()])

    def update(self, **fields):
        for k, v in fields.items():
            setattr(self, k, v)

    def __repr__(self):
        return ('<' + self.__class__.__name__ + ' ' + 
                      str(self.get_data()) + '>')

    @classmethod
    def create_entity(cls, identity, data={}):
        identity = cls.__flatten_identity(identity)
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Creating entity of type [%s] with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        try:
            cls.create_only_encoded(parent, identity, data)
        except etcd.exceptions.EtcdPreconditionException:
            pass
        else:
            return identity

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise ValueError("Entity [%s] identity with parent [%s] already "
                         "exists: %s" % 
                         (cls.entity_class, parent, identity))

    @classmethod
    def delete_entity(cls, identity):
        identity = cls.__flatten_identity(identity)
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Deleting entity of type [%s] with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        cls.delete(parent, identity)

    @classmethod
    def update_entity(cls, identity, data={}):
        identity = cls.__flatten_identity(identity)
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Updating entity of type [%s] with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        try:
            cls.update_only_encoded(parent, identity, data)
        except etcd.exceptions.EtcdPreconditionException:
            pass
        else:
            return identity

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise ValueError("Entity [%s] identity with parent [%s] doesn't "
                         "exist: %s" % 
                         (cls.entity_class, parent, identity))

    @classmethod
    def get_by_identity(cls, identity):
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Getting entity of type [%s] with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        return cls.get_encoded(parent, identity)

    @classmethod
    def get_children_identity(cls, partial_identity):
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Getting children entities of type [%s] with parent "
                      "[%s]: [%s]", 
                      cls.entity_class, parent, partial_identity)

        return cls.get_children_encoded(parent, partial_identity)

    @classmethod
    def make_opaque(cls):
        """Generate a unique ID string for the given type of identity."""

        # Our ID scheme is to generate SHA1s. Since these are going to be
        # unique, we'll just ignore entity_class.

        return hashlib.sha1(str(random.random()).encode('ASCII')).hexdigest()

    @classmethod
    def __flatten_key(cls, key):
        return '/' + '/'.join(key)

    @classmethod
    def __flatten_identity(cls, identity):
        """Derive a key from the identity string. It can either be a flat 
        string or a tuple of flat-strings. The latter will be collapsed to a
        path name (for heirarchical storage). We may or may not do something
        with hyphens in the future.
        """

        if issubclass(identity.__class__, tuple) is True:
            for part in identity:
                if '-' in part or \
                   '/' in part:
                    raise ValueError("Identity has reserved characters: %s" % 
                                     (identity,))

            return '/'.join(identity)
        else:
            if '-' in identity or \
               '/' in identity:
                raise ValueError("Identity has reserved characters: %s" % 
                                 (identity,))

            return identity

    @classmethod
    def __key_from_identity(cls, parent, identity):
        if issubclass(identity.__class__, tuple) is False:
            identity = (identity,)

        flat_key = cls.__flatten_key(parent + identity)

        _logger.debug("Flattened key: [%s] [%s] => [%s]", 
                      parent, identity, flat_key)

        return flat_key

    @classmethod
    def set(cls, parent, identity, value):
        return _etcd.node.set(
                cls.__key_from_identity(parent, identity), 
                value)

    @classmethod
    def set_encoded(cls, parent, identity, value):
        return _etcd.node.set(
                cls.__key_from_identity(parent, identity), 
                _encode(value))

    @classmethod
    def update(cls, parent, identity, value):
        return _etcd.node.set(
                cls.__key_from_identity(parent, identity), 
                value)

    @classmethod
    def update_encoded(cls, parent, identity, value):
        return _etcd.node.update_only(
                cls.__key_from_identity(parent, identity), 
                _encode(value))

    @classmethod
    def create_only(cls, parent, identity, value):
        return _etcd.node.create_only(
                cls.__key_from_identity(parent, identity), 
                value)

    @classmethod
    def create_only_encoded(cls, parent, identity, value):
        return _etcd.node.create_only(
                cls.__key_from_identity(parent, identity), 
                _encode(value))

    @classmethod
    def delete(cls, parent, identity):
        return _etcd.node.delete(cls.__key_from_identity(parent, identity))

# TODO(dustin): We might be fine assuming implicit directory-creation, but 
#               will have enough information to automate it if needed, later.
#
#    @classmethod
#    def directory_create_only(cls, parent, name):
#        return _etcd.directory.create_only(
#                cls.__key_from_identity(parent, name))

    @classmethod
    def get(cls, parent, identity):
        key = cls.__key_from_identity(parent, identity)
        response = _etcd.node.get(key)
        return response.node.value

    @classmethod
    def get_children(cls, parent, identity):
        key = cls.__key_from_identity(parent, identity)
        response = _etcd.node.get(key, recursive=True)
        for child in response.node.children:
            yield child.value

    @classmethod
    def get_encoded(cls, parent, identity):
        key = cls.__key_from_identity(parent, identity)
        response = _etcd.node.get(key)
        return _decode(response.node.value)

    @classmethod
    def get_children_encoded(cls, parent, identity):
        key = cls.__key_from_identity(parent, identity)

# TODO(dustin): We still need to confirm that we can get children without being recursive. Update the documentation, either way.
        response = _etcd.node.get(key)
        for child in response.node.children:
            # Don't just decode the data, but derive the identity for this 
            # child as well (subtract the search-key from the child-key).
            yield (child.key[len(key) + 1:], _decode(child.value))

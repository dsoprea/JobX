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


class Field(object):
    def __init__(self, is_required=True):
        self.__is_required = is_required

    @property
    def is_required(self):
        return self.__is_required


class Model(object):
    entity_class = None
    key_field = None

    def __init__(self, is_stored=False, *args, **fields):
        cls = self.__class__

        # If the key wasn't given, assign it randomly.
        if cls.key_field not in fields:
            key = cls.make_opaque()
            _logger.debug("Model [%s] was not loaded with key-field [%s]. "
                          "Generating key: [%s]", 
                          cls.entity_class, cls.key_field, key)

            fields[cls.key_field] = key

        # Make sure we received all of the fields.

        all_fields = set(self.__get_field_names())
        required_fields = set(self.__get_required_field_names())
        actual_fields = set(fields)

        assert actual_fields.issuperset(required_fields), \
               "Fields given [%s] do not subsume required fields [%s]." % \
               (actual_fields, required_fields)

        for missing_optional in (all_fields - required_fields):
            fields[missing_optional] = None

        for k, v in fields.items():
            setattr(self, k, v)

        # Reflects whether or not the data came from storage.
        self.__is_stored = is_stored

    def __repr__(self):
        cls = self.__class__

        return ('<%s [%s] %s>' % 
                (cls.__name__, getattr(self, cls.key_field), self.get_data()))

    def __get_required_field_names(self):
        cls = self.__class__

        for field_name in self.__get_field_names():
            if getattr(cls, field_name).is_required is True:
                yield field_name

    def __get_field_names(self):
        cls = self.__class__

        # We use None so we don't error with the private attributes.

        for attr in dir(self):
            if issubclass(getattr(cls, attr, None).__class__, Field):
                yield attr

    def get_data(self):
        cls = self.__class__

        return dict([(k, getattr(self, k)) 
                     for k 
                     in self.__get_field_names()
                     if k != cls.key_field])

    def get_key(self):
        cls = self.__class__

        return getattr(self, cls.key_field)

    def save(self):
        cls = self.__class__

        identity = self.get_identity()

        if self.__is_stored is True:
            cls.__update_entity(
                    identity, 
                    self.get_data())
        else:
            cls.__create_entity(
                    identity, 
                    self.get_data())

        self.__is_stored = True

    def delete(self):
        cls = self.__class__

        identity = self.get_identity()

        cls.__delete_entity(identity)
        self.__is_stored = False

    def list(self):
        cls = self.__class__
        return cls.__list_entities()

    def get_identity(self):
        raise NotImplementedError()

    @classmethod
    def get_and_build(cls, identity, key):
        data = cls.__get_entity(identity)
        data[cls.key_field] = key

        return cls(True, **data)

    @classmethod
    def make_opaque(cls):
        """Generate a unique ID string for the given type of identity."""

        # Our ID scheme is to generate SHA1s. Since these are going to be
        # unique, we'll just ignore entity_class.

        return hashlib.sha1(str(random.random()).encode('ASCII')).hexdigest()

    @classmethod
    def __create_entity(cls, identity, data={}):
        identity = cls.__flatten_identity(identity)
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Creating [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        try:
            cls.__create_only_encoded(parent, identity, data)
        except etcd.exceptions.EtcdPreconditionException:
            pass
        else:
            return identity

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise ValueError("[%s] entity identity with parent [%s] already "
                         "exists: [%s]" % 
                         (cls.entity_class, parent, identity))

    @classmethod
    def __update_entity(cls, identity, data={}):
        identity = cls.__flatten_identity(identity)
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Updating [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        try:
            cls.__update_only_encoded(parent, identity, data)
        except etcd.exceptions.EtcdPreconditionException:
            pass
        else:
            return identity

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise ValueError("[%s] entity identity with parent [%s] doesn't "
                         "exist: [%s]" % 
                         (cls.entity_class, parent, identity))

    @classmethod
    def __delete_entity(cls, identity):
        identity = cls.__flatten_identity(identity)
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Deleting [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        cls.__delete(parent, identity)

    @classmethod
    def __get_entity(cls, identity):
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Getting [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        return cls.__get_encoded(parent, identity)

    @classmethod
    def __get_encoded(cls, parent, identity):
        key = cls.__key_from_identity(parent, identity)
        response = _etcd.node.get(key)
        return _decode(response.node.value)

    @classmethod
    def __flatten_key(cls, key):
        return '/' + '/'.join(key)

#    @classmethod
#    def set(cls, parent, identity, value):
#        key = cls.__key_from_identity(parent, identity)
#        return _etcd.node.set(key, value)
#
    @classmethod
    def __update_only_encoded(cls, parent, identity, value):
        key = cls.__key_from_identity(parent, identity)
        return _etcd.node.update_only(key, _encode(value))

    @classmethod
    def __create_only_encoded(cls, parent, identity, value):
        key = cls.__key_from_identity(parent, identity)
        return _etcd.node.create_only(key, _encode(value))

    @classmethod
    def __delete(cls, parent, identity):
        key = cls.__key_from_identity(parent, identity)
        return _etcd.node.delete(key)

# TODO(dustin): We might be fine assuming implicit directory-creation, but 
#               will have enough information to automate it if needed, later.
#
#    @classmethod
#    def directory_create_only(cls, parent, name):
#        return _etcd.directory.create_only(
#                cls.__key_from_identity(parent, name))

    @classmethod
    def __list_entities(cls):
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Getting children [%s] entities with parent [%s].", 
                      cls.entity_class, parent)

        return cls.__list_encoded(parent, ())

    @classmethod
    def __list_encoded(parent, identity):
        key = cls.__key_from_identity(parent, identity)

# TODO(dustin): We still need to confirm that we can get children without being 
#               recursive. Update the documentation, either way.
        response = _etcd.node.get(key)
        for child in response.node.children:
            # Don't just decode the data, but derive the identity for this 
            # child as well (clip the search-key path-prefix from the child-key).
            yield (child.key[len(key) + 1:], _decode(child.value))

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
                    raise ValueError("Identity has reserved characters: [%s]" % 
                                     (identity,))

            return '/'.join(identity)
        else:
            if '-' in identity or \
               '/' in identity:
                raise ValueError("Identity has reserved characters: [%s]" % 
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

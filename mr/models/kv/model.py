import logging
import json
import random
import hashlib

import etcd.client
import etcd.exceptions

import mr.config.etcd

logging.getLogger('etcd').setLevel(logging.INFO)

_ENTITY_ROOT = 'entities'
_REPR_DATA_TRUNCATE_WIDTH = 20

_logger = logging.getLogger(__name__)

_encode = lambda data: json.dumps(data)
_decode = lambda encoded: json.loads(encoded)

_etcd = etcd.client.Client(**mr.config.etcd.CLIENT_CONFIG)


class ValidationError(Exception):
    def __init__(self, name, message):
        extended_message = ("%s: %s" % (name, message))
        super(ValidationError, self).__init__(extended_message)


class Field(object):
    def __init__(self, is_required=True):
        self.__is_required = is_required

    def validate(self, name, value):
        """Raise ValidationError on error."""

        pass

    def is_empty(self, value):
        return not value

    @property
    def is_required(self):
        return self.__is_required

# We want to get everything using this as the default (not Field).
TextField = Field


class EnumField(Field):
    def __init__(self, values, *args, **kwargs):
        super(EnumField, self).__init__(*args, **kwargs)

        self.__values = values

    def validate(self, name, value):
        if value not in self.__values:
            raise ValidationError(name, "Not a valid enum value.")


class Model(object):
    entity_class = None
    key_field = None

    def __init__(self, is_stored=False, *args, **data):
        self.__load_from_data(data, is_stored=is_stored)

    def __load_from_data(self, data, is_stored=False):
        cls = self.__class__

        # If the key wasn't given, assign it randomly.
        if cls.key_field not in data:
            key = cls.make_opaque()
            _logger.debug("Model [%s] was not loaded with key-field [%s]. "
                          "Generating key: [%s]", 
                          cls.entity_class, cls.key_field, key)

            data[cls.key_field] = key

        # Make sure we received all of the fields.

        all_fields = set(self.__get_field_names())
        actual_fields = set(data.keys())

        # Make sure that only valid fields were given.

        invalid_fields = actual_fields - all_fields
        if invalid_fields:
            raise ValueError("Invalid fields were given: %s" % (invalid_fields,))

        # Fill-in any missing fields.

        for field_name in (all_fields - actual_fields):
            data[field_name] = None

        # Determine which fields had empty data.

        data_info = [(field, 
                      getattr(self, field), 
                      data[field]) 
                     for field 
                     in all_fields]

        for name, field_obj, datum in data_info:
            if field_obj.is_required and field_obj.is_empty(datum):
                raise ValidationError(name, "Required field is empty/omitted")

            field_obj.validate(name, datum)

        for k, v in data.items():
            setattr(self, k, v)

        # Reflects whether or not the data came from storage.
        self.__is_stored = is_stored

    def __repr__(self):
        cls = self.__class__

        truncated_data = {}
        for k, v in self.get_data().items():
            if issubclass(v.__class__, (basestring, unicode)) is True and \
               len(v) > _REPR_DATA_TRUNCATE_WIDTH:
                v = v[:_REPR_DATA_TRUNCATE_WIDTH] + '...'

            truncated_data[k] = v

        return ('<%s [%s] %s>' % 
                (cls.__name__, getattr(self, cls.key_field), truncated_data))

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

    def presave(self):
        pass

    def save(self, check_index=False):
        cls = self.__class__

        self.presave()

        identity = self.get_identity()

        if self.__is_stored is True:
            cls.__update_entity(
                    identity, 
                    self.get_data(),
                    check_index=check_index)
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

    def refresh(self):
        cls = self.__class__

        (attributes, data) = cls.__get_entity(self.get_identity())
        data[cls.key_field] = self.get_key()

        self.__load_from_data(data, is_stored=True)
        self.__class__.__apply_attributes(self, attributes)

    def get_identity(self):
        raise NotImplementedError()

    @classmethod
    def __build_from_data(cls, key, data):
        data[cls.key_field] = key
        return cls(True, **data)

    @classmethod
    def __apply_attributes(cls, obj, attributes):
        obj.__index = attributes['modified_index']

    @classmethod
    def get_and_build(cls, identity, key):
        (attributes, data) = cls.__get_entity(identity)

        obj = cls.__build_from_data(key, data)
        cls.__apply_attributes(obj, attributes)

        return obj

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
    def __update_entity(cls, identity, data={}, check_index=False):
        identity = cls.__flatten_identity(identity)
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Updating [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        try:
            cls.__update_only_encoded(
                parent, 
                identity, 
                data, 
                check_index=check_index)
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

        return (
            {
                'modified_index': response.node.modified_index, 
            },
            _decode(response.node.value)
        )

    @classmethod
    def __flatten_key(cls, key):
        return '/' + '/'.join(key)

#    @classmethod
#    def set(cls, parent, identity, value):
#        key = cls.__key_from_identity(parent, identity)
#        return _etcd.node.set(key, value)
#
    @classmethod
    def __update_only_encoded(cls, parent, identity, value, check_index=False):
        key = cls.__key_from_identity(parent, identity)

        # If check_index is set, make sure the node hasn't changed since we 
        # read it.
        current_index = self.__index if check_index is True else None

        return _etcd.node.compare_and_swap(
                    key, 
                    value, 
                    prev_exists=True, 
                    current_index=current_index)

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
    def list(cls, *args):
        parent = (_ENTITY_ROOT, cls.entity_class)

        _logger.debug("Getting children [%s] entities of parent [%s].", 
                      cls.entity_class, parent)

        for key, data in cls.__list_encoded(parent, args):
            yield cls.__build_from_data(key, data)

    @classmethod
    def __list_encoded(cls, parent, identity_prefix):
        key = cls.__key_from_identity(parent, identity_prefix)

# TODO(dustin): We still need to confirm that we can get children without being 
#               recursive. Update the documentation, either way.
        response = _etcd.node.get(key)
        for child in response.node.children:
            # Don't just decode the data, but derive the identity for this 
            # child as well (clip the search-key path-prefix from the child-key).
            yield (child.key[len(key) + 1:], _decode(child.value))

    @classmethod
    def list_keys(cls, *args):
# TODO(dustin): Currently, we have to receive the compete response from *etcd* 
#               before we can operate on it. That response also includes 
#               values, which we don't need at all right here. This needs to be 
#               an improvement within *etcd*.
        parent = (_ENTITY_ROOT, cls.entity_class)
        key = cls.__key_from_identity(parent, args)

        response = _etcd.node.get(key)
        for child in response.node.children:
            yield child.key[len(key) + 1:]

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

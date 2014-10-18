import logging
import json
import random
import hashlib
import collections
import datetime

import etcd.exceptions

import mr.constants
import mr.config.kv
import mr.models.kv.common
import mr.models.kv.data_layer
import mr.compat

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


class ValidationError(Exception):
    def __init__(self, name, message):
        extended_message = ("%s: %s" % (name, message))
        super(ValidationError, self).__init__(extended_message)


class Field(object):
    def __init__(self, is_required=True, default_value=None, empty_value=None):
# TODO(dustin): Check the existing field assignments to determine if we need to adjust their default_value or empty_value parameters (if given).
        self.__is_required = is_required
        self.__default_value = default_value
        self.__empty_value = empty_value

    def validate(self, name, value):
        """Raise ValidationError on error."""

        pass

    def is_empty(self, value):
        return value == self.__empty_value or value is None

    @property
    def is_required(self):
        return self.__is_required

    @property
    def default_value(self):
        return self.__default_value

# We want to get everything using this as the default (not Field).
TextField = Field


class EnumField(Field):
    def __init__(self, values, *args, **kwargs):
        super(EnumField, self).__init__(*args, **kwargs)

        self.__values = values

    def validate(self, name, value):
        if value not in self.__values:
            raise ValidationError(name, "Not a valid enum value.")


class TimestampField(Field):
    def validate(self, name, value):
        try:
            datetime.datetime.strptime(value, mr.constants.DATETIME_STD)
        except ValueError:
            pass
        else:
            return

        raise ValidationError(name, "Timestamp is not properly formatted.")

    @property
    def default_value(self):
        return datetime.datetime.now().strftime(mr.constants.DATETIME_STD)

_dl = mr.models.kv.data_layer.DataLayerKv()


class Model(mr.models.kv.common.CommonKv):
    entity_class = None
    key_field = None

    def __init__(self, is_stored=False, *args, **data):
        assert issubclass(is_stored.__class__, bool) is True

        _logger.debug("Instantiating [%s]. IS_STORED=[%s]", 
                      self.__class__.__name__, is_stored)

        self.__state = None

        self.__load_from_data(data, is_stored=is_stored)

    def __load_from_data(self, data, is_stored=False):
        cls = self.__class__

        _logger.debug("Loading data on model [%s]. IS_STORED=[%s]", 
                      cls.__name__, is_stored)

        try:
            # If the key wasn't non-None, assign it randomly.
            #
            # Previously, we allowed this column to be optional, but we'd get 
            # spurious keys without realizing it.
            if data[cls.key_field] is None:
                key = cls.make_opaque()
                _logger.debug("Model [%s] was not loaded with a key. Generating "
                              "key: [%s]", cls.entity_class, key)

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
                data[field_name] = getattr(cls, field_name).default_value

            # Determine which fields had empty data.

            data_info = [(field_name, 
                          getattr(cls, field_name), 
                          data[field_name]) 
                         for field_name 
                         in all_fields]

            for name, field_obj, datum in data_info:
                if field_obj.is_empty(datum) is False:
                    field_obj.validate(name, datum)
                elif field_obj.is_required:
                    raise ValidationError(name, "Required field is empty/omitted")

                setattr(self, name, datum)

            # Reflects whether or not the data came from storage.
            self.__is_stored = is_stored
        except:
            try:
                pk_value = data[cls.key_field]
            except KeyError:
                pk_value = None

            _logger.exception("There was an error while loading data for "
                              "model [%s]. PK=[%s]", cls.__name__, pk_value)

            raise

    def __str__(self):
        cls = self.__class__

        return ('<%s>' % (getattr(self, cls.key_field),))

    def __repr__(self):
        cls = self.__class__

        truncated_data = {}
        for k, v in self.get_data().iteritems():
            if issubclass(v.__class__, mr.compat.basestring) is True and \
               len(v) > mr.config.kv.REPR_DATA_TRUNCATE_WIDTH:
                v = v[:mr.config.kv.REPR_DATA_TRUNCATE_WIDTH] + '...'

            truncated_data[k] = v

        return ('<%s [%s] %s>' % 
                (cls.__name__, getattr(self, cls.key_field), truncated_data))

    def get_debug(self, ignore_keys=()):
        cls = self.__class__

        data = dict([(k, v) 
                     for k, v 
                     in self.get_data().iteritems() 
                     if k != cls.key_field and \
                        k not in ignore_keys])

        info = collections.OrderedDict()
        info['entity_name'] = cls.__name__
        info['primary_key'] = collections.OrderedDict()

        info['primary_key'][cls.key_field] = getattr(self, cls.key_field)
        info['data'] = data

        return json.dumps(
                info, 
                indent=4, 
                separators=(',', ': '))

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
        """Return a dictionary of data. If the value is considered to be
        "empty" by the particular field-type, then coalesce it to whatever the 
        default-value for each field-type is, e.g. both None and ''.
        """

        cls = self.__class__

        data = []
        for k in self.__get_field_names():
            if k == cls.key_field:
                continue

            datum = getattr(self, k)
            field_obj = getattr(cls, k)
            if getattr(cls, k).is_empty(datum) is True:
                datum = field_obj.default_value

            data.append((k, datum))

        return dict(data)

    def get_key(self):
        cls = self.__class__

        return getattr(self, cls.key_field)

    def presave(self):
        pass

    def postsave(self):
        pass

    def predelete(self):
        pass

    def postdelete(self):
        pass

    @classmethod
    def atomic_update(cls, get_cb, set_cb, 
                      max_attempts=\
                        mr.config.kv.DEFAULT_ATOMIC_UPDATE_MAX_ATTEMPTS):
# TODO(dustin): This functionality is now native to the client (the *node* 
#               module).
        i = max_attempts
        while i > 0:
            obj = get_cb()

            try:
                set_cb(obj)
                obj.save(enforce_pristine=True)
            except mr.models.kv.data_layer.KvPreconditionException:
                obj.refresh()
            else:
                return obj

            i -= 1

        raise SystemError("Atomic update failed: %s" % (obj,))

    def save(self, enforce_pristine=False):
        cls = self.__class__

        self.presave()

        identity = self.get_identity()

        _logger.debug("Saving model [%s]. IS_STORED=[%s]", 
                      cls.__name__, self.__is_stored)


        if self.__is_stored is True:
            state = self.__state if enforce_pristine is True else None

            cls.__update_entity(
                    identity, 
                    self.get_data(),
                    check_against_state=state)
        else:
            state = cls.__create_entity(
                        identity, 
                        self.get_data())

            attributes = {
                'state': str(state), 
            }

            self.__class__.__apply_attributes(self, attributes)
            self.__is_stored = True

        self.postsave()

    def delete(self):
        cls = self.__class__

        self.predelete()

        identity = self.get_identity()

        cls.delete_entity(identity)
        self.__is_stored = False

        self.postdelete()

    def refresh(self):
        cls = self.__class__

        identity = self.get_identity()
        key = getattr(self, cls.key_field)

        assert identity is not None
        assert key is not None

        _logger.debug("Refreshing entity with identity and key: [%s] [%s]", 
                      identity, key)

        (attributes, data) = cls.__get_entity(identity)
        data[cls.key_field] = key

        self.__load_from_data(data, is_stored=True)
        self.__class__.__apply_attributes(self, attributes)

    @property
    def is_stored(self):
        return self.__is_stored

    @property
    def state_string(self):
        return self.__state

    def get_identity(self):
        raise NotImplementedError()

    @classmethod
    def __build_from_stored_data(cls, key, data):
        data[cls.key_field] = key
        return cls(is_stored=True, **data)

    @classmethod
    def __apply_attributes(cls, obj, attributes):
        # This won't be set prior to this. There's no benefit to it having a 
        # None value, and might potentially be confusing.
        obj.__state = attributes['state']

    @classmethod
    def get_and_build(cls, identity, key):
        (attributes, data) = cls.__get_entity(identity)

        obj = cls.__build_from_stored_data(key, data)
        cls.__apply_attributes(obj, attributes)

        return obj

    @classmethod
    def __create_entity(cls, identity, data={}):
        parent = mr.config.kv.ENTITY_ROOT + (cls.entity_class,)

        _logger.debug("Creating [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        try:
            return cls.__create_only_encoded(parent, identity, data)
        except etcd.exceptions.EtcdPreconditionException:
            pass

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging (no exception-from-exception messages).
        raise ValueError("[%s] entity identity with parent [%s] already "
                         "exists: [%s]" % 
                         (cls.entity_class, parent, identity))

    @classmethod
    def __update_entity(cls, identity, data={}, check_against_state=None):
        parent = mr.config.kv.ENTITY_ROOT + (cls.entity_class,)

        _logger.debug("Updating [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        try:
            cls.__update_only_encoded(
                parent, 
                identity, 
                data, 
                check_against_state=check_against_state)
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
    def delete_entity(cls, identity):
        parent = mr.config.kv.ENTITY_ROOT + (cls.entity_class,)

        _logger.debug("Deleting [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        cls.__delete(parent, identity)

    @classmethod
    def __get_entity(cls, identity):
        parent = mr.config.kv.ENTITY_ROOT + (cls.entity_class,)

        _logger.debug("Getting [%s] entity with parent [%s]: [%s]", 
                      cls.entity_class, parent, identity)

        return cls.__get_encoded(parent, identity)

    @classmethod
    def __get_encoded(cls, parent, identity):
        key = cls.key_from_identity(parent, identity)
        
        (state, value) = _dl.get(key)

        return (
            {
                'state': str(state), 
            },
            mr.config.kv.DECODER(value)
        )

    def wait_for_change(self):
        cls = self.__class__

        parent = mr.config.kv.ENTITY_ROOT + (cls.entity_class,)
        identity = self.get_identity()
        key = getattr(self, cls.key_field)

        _logger.debug("Waiting on entity [%s] with parent [%s]: [%s]",
                      cls.entity_class, parent, identity)

        (attributes, data) = cls.__wait_encoded(parent, identity)

        obj = cls.__build_from_stored_data(key, data)
        cls.__apply_attributes(obj, attributes)

        return obj

    @classmethod
    def __wait_encoded(cls, parent, identity):
        key = cls.key_from_identity(parent, identity)
        
        (state, value) = _dl.wait(key)

        return (
            {
                'state': str(state), 
            },
            mr.config.kv.DECODER(value)
        )

    @classmethod
    def __update_only_encoded(cls, parent, identity, value, 
                              check_against_state=None):
        key = cls.key_from_identity(parent, identity)

        return _dl.update_only(
                key, 
                mr.config.kv.ENCODER(value), 
                check_against_state=check_against_state)

    @classmethod
    def __create_only_encoded(cls, parent, identity, value):
        key = cls.key_from_identity(parent, identity)
        return _dl.create_only(key, mr.config.kv.ENCODER(value))

    @classmethod
    def __delete(cls, parent, identity):
        key = cls.key_from_identity(parent, identity)
        return _dl.delete(key)

    @classmethod
    def list(cls, *args):
        parent = mr.config.kv.ENTITY_ROOT + (cls.entity_class,)

        _logger.debug("Getting children [%s] entities of parent [%s].", 
                      cls.entity_class, parent)

        for key, data in cls.__list_encoded(parent, args):
            yield cls.__build_from_stored_data(key, data)

    @classmethod
    def __list_encoded(cls, parent, identity_prefix):
        key = cls.key_from_identity(parent, identity_prefix)

        for name, data in _dl.list(key):
            # Don't just decode the data, but derive the identity for this 
            # child as well (clip the search-key path-prefix from the child-key).
            yield (name, mr.config.kv.DECODER(data))

    @classmethod
    def list_children(cls, *args):
        parent = mr.config.kv.ENTITY_ROOT + (cls.entity_class,)
        key = cls.key_from_identity(parent, args)

        return _dl.list_children(key)

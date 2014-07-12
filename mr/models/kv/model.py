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

class KvModel(object):
    entity_class = None

    def __init__(self):
        self.__etcd = etcd.client.Client(**mr.config.etcd.CLIENT_CONFIG)

    def create_entity(self, identity, data={}):
        identity = self.__flatten_identity(identity)
        parent = (_ENTITY_ROOT, self.__class__.entity_class)

        _logger.debug("Creating entity of type [%s] with parent [%s]: [%s]", 
                      self.__class__.entity_class, parent, identity)

        try:
            self.create_only_encoded(parent, identity, data)
        except etcd.exceptions.EtcdPreconditionException:
            pass
        else:
            return identity

        # Re-raising here rather than in the catch above makes for cleaner 
        # logging.
        raise ValueError("Entity [%s] identity with parent [%s] already "
                         "exists: %s" % 
                         (self.__class__.entity_class, parent, identity))

    def get_by_identity(self, identity):
        parent = (_ENTITY_ROOT, self.__class__.entity_class)

        _logger.debug("Getting entity of type [%s] with parent [%s]: [%s]", 
                      self.__class__.entity_class, parent, identity)

        return self.get_encoded(parent, identity)

    def make_opaque(self):
        """Generate a unique ID string for the given type of identity."""

        # Our ID scheme is to generate SHA1s. Since these are going to be
        # unique, we'll just ignore entity_class.

        return hashlib.sha1(str(random.random()).encode('ASCII')).hexdigest()

    def __flatten_key(self, key):
        return '/' + '/'.join(key)

    def __flatten_identity(self, identity):
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

    def __key_from_identity(self, parent, identity):
        if issubclass(identity.__class__, tuple) is False:
            identity = (identity,)

        return self.__flatten_key(parent + identity)

    def set(self, parent, identity, value):
        return self.__etcd.node.set(
                self.__key_from_identity(parent, identity), 
                value)

    def set_encoded(self, parent, identity, value):
        return self.__etcd.node.set(
                self.__key_from_identity(parent, identity), 
                _encode(value))


    def create_only(self, parent, identity, value):
        return self.__etcd.node.create_only(
                self.__key_from_identity(parent, identity), 
                value)

    def create_only_encoded(self, parent, identity, value):
        return self.__etcd.node.create_only(
                self.__key_from_identity(parent, identity), 
                _encode(value))

# TODO(dustin): We might be fine assuming implicit directory-creation, but 
#               will have enough information to automate it if needed, later.
#
#    def directory_create_only(self, parent, name):
#        return self.__etcd.directory.create_only(
#                self.__key_from_identity(parent, name))

    def get(self, parent, identity):
        key = self.__key_from_identity(parent, identity)
        response = self.__etcd.node.get(key)
        return response.node.value

    def get_encoded(self, parent, identity):
        key = self.__key_from_identity(parent, identity)
        response = self.__etcd.node.get(key)
        return _decode(response.node.value)

    @property
    def client(self):
        return self.__etcd

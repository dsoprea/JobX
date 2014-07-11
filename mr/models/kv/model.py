import logging
import json
import random
import hashlib

import etcd

import mr.config.etcd

_ENTITY_ROOT = 'entities'

_logger = logging.getLogger(__name__)


class KvModel(object):
    entity_class = None

    def __init__(self):
        self.__etcd = etcd.Client(**mr.config.etcd.CLIENT_CONFIG)

    def create_entity(self, identity=None, data={}, parent=None):
        if identity is None:
            is_opaque = True
            identity = self.make_opaque()
        else:
            is_opaque = False

        if parent is not None:
            key = (_ENTITY_ROOT, self.__class__.entity_class) + \
                  parent + \
                  (identity,)
        else:
            key = (_ENTITY_ROOT, self.__class__.entity_class, identity)

        _logger.debug("Creating identity of type [%s]: [%s] OPAQUE=[%s]", 
                      self.__class__.entity_class, identity, is_opaque)

        self.create_only_encoded(key, data)

        return identity

    def create_directory(self, identity=None, parent=None):
        if identity is None:
            is_opaque = True
            identity = self.make_opaque()
        else:
            is_opaque = False

        if parent is not None:
            key = (_ENTITY_ROOT, self.__class__.entity_class) + \
                  parent + \
                  (identity,)
        else:
            key = (_ENTITY_ROOT, self.__class__.entity_class, identity)

        _logger.debug("Creating identity of type [%s]: [%s] OPAQUE=[%s]", 
                      self.__class__.entity_class, identity, is_opaque)

        self.directory_create_only(key)

        return identity

    def get_by_identity(self, id_):
        key = (_ENTITY_ROOT, self.__class__.entity_class, id_)
        return self.get_encoded(key)

    def make_opaque(self):
        """Generate a unique ID string for the given type of identity."""

        # Our ID scheme is to generate SHA1s. Since these are going to be
        # unique, we'll just ignore entity_class.

        return hashlib.sha1(random.random()).hexdigest()

    def __flatten_key(self, key):
        return '/' + '/'.join(key)

    def set(self, key, value):
        return self.__etcd.node.set(
                self.__flatten_key(key), 
                value)

    def set_encoded(self, key, value):
        return self.__etcd.node.set(
                self.__flatten_key(key), 
                json.dumps(value))


    def create_only(self, key, value):
        return self.__etcd.node.create_only(
                self.__flatten_key(key), 
                value)

    def create_only_encoded(self, key, value):
        return self.__etcd.node.create_only(
                self.__flatten_key(key), 
                json.dumps(value))

    def directory_create_only(self, key):
        return self.__etcd.directory.create_only(
                self.__flatten_key(key))

    def get(self, key):
        response = self.__etcd.node.get(self.__flatten_key(key))
        return response.node.value

    def get_encoded(self, key):
        response = self.__etcd.node.get(self.__flatten_key(key))
        return json.loads(response.node.value)

    @property
    def client(self):
        return self.__etcd

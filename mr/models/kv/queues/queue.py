import logging

import mr.config
import mr.config.kv
import mr.models.kv.data_layer

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


class Queue(mr.models.kv.data_layer.QueueLayerKv):
    queue_class = None

    def __init__(self, log_key=None, *args, **kwargs):
        root_identity = self.__get_root_identity()
        super(Queue, self).__init__(root_identity, *args, **kwargs)

        self.__log_key = log_key

    def __repr__(self):
        cls = self.__class__

        return ('<%s %s>' % (cls.__name__, self.__get_root_identity()))

    def __write_log(self, message, *args):
#        if self.__log_key is not None and mr.config.IS_DEBUG is True:
#            _logger.debug("Dataset(%s): " + message, self.__log_key, *args)
        pass

    def get_root_tree_identity(self):
        """Returns a complete tuple that'll be flattened to the path that 
        contains the children.
        """

        raise NotImplementedError()

    def get_entity_from_data(self, data):
        """Returns the model object for the given child."""

        raise NotImplementedError()

    def get_data_from_entity(self, entity):
        """Derive the name/key from the given entity, with which to represent 
        the child.
        """

        raise NotImplementedError()

    def __get_root_identity(self):
        cls = self.__class__

        try:
            return self.__root_identity
        except AttributeError:
            root_tree_identity = self.get_root_tree_identity()

            if issubclass(root_tree_identity.__class__, tuple) is False:
                root_tree_identity = (root_tree_identity,)

            if cls.queue_class is None:
                raise ValueError("queue_class is not defined.")

            self.__root_identity = mr.config.kv.QUEUE_ROOT + \
                                   root_tree_identity

            _logger.debug("Building root-identity: [%s] [%s]", 
                          cls.queue_class, self.__root_identity)

            return self.__root_identity

    def add(self, data):
        self.__write_log("add: %s", data)

        encoded_data = mr.config.kv.ENCODER(data)
        return super(Queue, self).add(encoded_data)

    def add_entity(self, entity):
        self.__write_log("add_entity: %s", entity)

        data = self.get_data_from_entity(entity)
        return self.add(data)

    def get(self, key):
        self.__write_log("get: %s", key)

        (state, encoded_data) = super(Queue, self).get(key)
        return mr.config.kv.DECODER(encoded_data)

    def get_entity(self, key):
        self.__write_log("get_entity: %s", key)

        encoded_data = self.get(key)
        data = mr.config.kv.DECODER(encoded_data)
        return self.get_entity_from_data(data)

    def list_data(self, head_count=None):
        """Used if data was stored to the entry."""

        self.__write_log("list_data")

# TODO(dustin): We'll have to use paging as implemented by *etcd* when, er, implement it.
        i = 0
        for encoded_data in super(Queue, self).list_data():
            if head_count is not None and i >= head_count:
                break

            yield mr.config.kv.DECODER(encoded_data)
            i += 1

    def list_keys_with_data(self, head_count=None):
        """Used if data was stored to the entry."""

        self.__write_log("list_keys_with_data")

# TODO(dustin): We'll have to use paging as implemented by *etcd* when, er, implement it.
        i = 0
        for key, encoded_data in super(Queue, self).list():
            if head_count is not None and i >= head_count:
                break

            yield (key, mr.config.kv.DECODER(encoded_data))
            i += 1

    def list_entities(self, head_count=None):
        """Used if an entity was encoded and stored as the data."""

        self.__write_log("list_entities")

# TODO(dustin): We'll have to use paging as implemented by *etcd* when, er, implement it.
        i = 0
        for encoded_data in self.list_data():
            if head_count is not None and i >= head_count:
                break

            data = mr.config.kv.DECODER(encoded_data)
            yield self.get_entity_from_data(data)
            i += 1

    def list_keys_with_entities(self, head_count=None):
        """Used if an entity was encoded and stored as the data."""

        self.__write_log("list_keys_with_entities")

# TODO(dustin): We'll have to use paging as implemented by *etcd* when, er, implement it.
        i = 0
        for key, encoded_data in self.list():
            if head_count is not None and i >= head_count:
                break

            data = mr.config.kv.DECODER(encoded_data)
            yield (key, self.get_entity_from_data(data))
            i += 1

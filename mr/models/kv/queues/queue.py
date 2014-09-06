import logging

import mr.config.kv
import mr.models.kv.data_layer

_logger = logging.getLogger(__name__)
#_logger.setLevel(logging.INFO)

#_dl = mr.models.kv.data_layer.QueueLayerKv()


class Queue(mr.models.kv.data_layer.QueueLayerKv):
    queue_class = None

    def __init__(self, *args, **kwargs):
        root_identity = self.__get_root_identity()
        super(Queue, self).__init__(root_identity, *args, **kwargs)

    def __repr__(self):
        cls = self.__class__

        return ('<%s %s>' % (cls.__name__, self.__get_root_identity()))

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
        encoded_data = mr.config.kv.ENCODER(data)
        return super(Queue, self).add(encoded_data)

    def add_entity(self, entity):
        data = self.get_data_from_entity(entity)
        return self.add(data)

    def get(self, key):
        (state, encoded_data) = super(Queue, self).get(key)
        return mr.config.kv.DECODER(encoded_data)

    def get_entity(self, key):
        data = self.get(key)
        return self.get_entity_from_data(data)

    def list(self):
        for key, encoded_data in super(Queue, self).list():
            yield (key, mr.config.kv.DECODER(encoded_data))

    def list_data(self):
        for encoded_data in super(Queue, self).list_data():
            yield mr.config.kv.DECODER(encoded_data)

    def list_entities(self):
        for key, data in self.list():
            yield (key, self.get_entity_from_data(data))

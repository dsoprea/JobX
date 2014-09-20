import logging

import mr.config.kv
import mr.models.kv.data_layer

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

# TODO(dustin): This is a base-class. We need to be able to represent trees of 
#               models, where each of these mappings don't necessarily have an 
#               identity of their own. We need to be able to create them, add 
#               to them, retrieve on the individual children, list the 
#               children, and delete them.
#
#               - They don't need very elaborate logic.
#               - We don't plan on storing the children information in the 
#                 logic. They'll always be retrieved from the KV.
#

_dl = mr.models.kv.data_layer.DataLayerKv()


class Tree(mr.models.kv.common.CommonKv):
    tree_class = None

    def __repr__(self):
        cls = self.__class__

        return ('<%s %s>' % (cls.__name__, self.__get_root_identity()))

    def get_root_tree_identity(self):
        """Returns a complete tuple that'll be flattened to the path that 
        contains the children.
        """

        raise NotImplementedError()

    def get_child_model_entity(self, child_name):
        """Returns the model object for the given child."""

        raise NotImplementedError()

    def get_name_from_child_entity(self, entity):
        """Derive the name/key from the given entity, with which to represent 
        the child.
        """

        raise NotImplementedError()

    def __get_root_identity(self):
        try:
            return self.__root_identity
        except AttributeError:
            root_tree_identity = self.get_root_tree_identity()

            if issubclass(root_tree_identity.__class__, tuple) is False:
                root_tree_identity = (root_tree_identity,)

            if self.__class__.tree_class is None:
                raise ValueError("tree_class is not defined.")

            self.__root_identity = mr.config.kv.ENTITY_TREE_ROOT + \
                                   root_tree_identity

            _logger.debug("Building root-identity: [%s] [%s]", 
                          self.__class__.tree_class, self.__root_identity)

            return self.__root_identity

    def add(self, name, data={}):
        identity = self.__get_root_identity() + (name,)
        return _dl.create_only(identity, mr.config.kv.ENCODER(data))

    def set(self, name, data={}):
        identity = self.__get_root_identity() + (name,)
        return _dl.set(identity, mr.config.kv.ENCODER(data))

    def get(self, name):
        identity = self.__get_root_identity() + (name,)
        (state, encoded_data) = _dl.get(identity)
        return mr.config.kv.DECODER(encoded_data)

    def get_data_for_entity(self, entity):
        name = self.get_name_from_child_entity(entity)
        identity = self.__get_root_identity() + (name,)
        (state, encoded_data) = _dl.get(identity)
        return mr.config.kv.DECODER(encoded_data)

    def add_entity(self, entity, data={}):
        name = self.get_name_from_child_entity(entity)
        return self.add(name, data)

    def update(self, name, data={}):
        identity = self.__get_root_identity() + (name,)
        return _dl.update_only(identity, mr.config.kv.ENCODER(data))

    def update_entity(self, entity, data={}):
        name = self.get_name_from_child_entity(entity)
        return self.update(name, data)

    def list(self):
        identity = self.__get_root_identity()
        for name, encoded_data in _dl.list(identity):
            yield (name, mr.config.kv.DECODER(encoded_data))

    def list_keys(self):
        """Yield each the path names of each child."""

        identity = self.__get_root_identity()
        return _dl.list_keys(identity)

    def list_data(self):
        identity = self.__get_root_identity()
        for name, encoded_data in _dl.list(identity):
            yield mr.config.kv.DECODER(encoded_data)

    def list_entities(self):
        identity = self.__get_root_identity()
        for name in _dl.list_keys(identity):
            yield self.get_child_model_entity(name)

    def list_entities_and_data(self):
        identity = self.__get_root_identity()
        return ((self.get_child_model_entity(name), 
                 mr.config.kv.DECODER(encoded_data)) 
                for name, encoded_data 
                in _dl.list(identity))

    def create(self):
        identity = self.__get_root_identity()
        _dl.directory_create_only(identity)

    def delete(self):
        identity = self.__get_root_identity()
        _dl.directory_delete(identity)

    def exists(self):
        identity = self.__get_root_identity()
        
        try:
            _dl.directory_exists(identity)
        except KeyError:
            return False
        else:
            return True

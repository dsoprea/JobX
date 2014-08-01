

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


class Tree(object):
    parent_model = None
    child_model = None

    @classmethod
    def create(cls, parent_key):
# TODO(dustin): Finish.
        raise NotImplementedError()

    def add_child(self, obj):
# TODO(dustin): Finish.
        raise NotImplementedError()

    def list_children(self):
# TODO(dustin): Finish.
        raise NotImplementedError()

import mr.models.kv.trees.tree
import mr.models.kv.invocation

# Relationship-types.
RT_MAPPED = 'mapped'
RT_REDUCED = 'reduced'


class RelationshipsTree(mr.models.kv.trees.tree.Tree):
    tree_class = 'relationships'

    def __init__(self, workflow, from_invocation, relationship_type):
        assert issubclass(
                from_invocation.__class__, 
                mr.models.kv.invocation.Invocation)

        self.__workflow = workflow
        self.__from_invocation = from_invocation
        self.__relationship_type = relationship_type

    def get_root_tree_identity(self):
        """Returns a complete tuple that'll be flattened to the path that 
        contains the children.
        """

        return (self.__class__.tree_class, 
                self.__workflow.workflow_name, 
                self.__from_invocation.invocation_id,
                self.__relationship_type)

    def get_child_model_entity(self, invocation_id):
        """Returns the model object for the given child."""

        return mr.models.kv.invocation.get(self.__workflow, invocation_id)

    def get_name_from_child_entity(self, invocation):
        """Derive the name/key from the given entity, with which to represent 
        the child.
        """

        return invocation.invocation_id

import mr.models.kv.trees.tree
import mr.models.kv.invocation


class MappedStepsTree(mr.models.kv.trees.tree.Tree):
    tree_class = 'mapped_steps'

    def __init__(self, workflow, parent_invocation):
        self.__workflow = workflow
        self.__parent_invocation = parent_invocation

    def get_root_tree_identity(self):
        """Returns a complete tuple that'll be flattened to the path that 
        contains the children.
        """

        return (self.__workflow.workflow_name, 
                self.__class__.tree_class, 
                self.__parent_invocation.invocation_id)

    def get_child_model_entity(self, child_name):
        """Returns the model object for the given child."""

        return mr.models.kv.invocation.get(self.__workflow, child_name)

    def get_name_from_child_entity(self, invocation):
        """Derive the name/key from the given entity, with which to represent 
        the child.
        """

        return invocation.invocation_id

import mr.models.kv.trees.tree
import mr.models.kv.invocation


class SessionsTree(mr.models.kv.trees.tree.Tree):
    tree_class = 'sessions'

    def __init__(self, workflow, map_invocation):
        assert issubclass(
                map_invocation.__class__, 
                mr.models.kv.invocation.Invocation)

        self.__workflow = workflow
        self.__map_invocation = map_invocation

    def get_root_tree_identity(self):
        """Returns a complete tuple that'll be flattened to the path that 
        contains the children.
        """

        return (self.__class__.tree_class, 
                self.__workflow.workflow_name, 
                self.__map_invocation.invocation_id)

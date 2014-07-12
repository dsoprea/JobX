# TODO(dustin): We also need a way to resume an incomplete workflow invocation.

# TODO(dustin): This should probably be a model. We can represent steps of 
#               steps as a path heirarchy.

class _InvokedStep(object):
    def __init__(self, invoked_step_id, invoked_workflow, step, arguments, 
                 parent_invocation=None):

# TODO(dustin): Finish.
# TODO(dustin): Arguments should be serializable.

        self.__invoked_step_id = invoked_step_id
        self.__invoked_workflow = invoked_workflow
        self.__step = step
        self.__arguments = arguments
        self.__parent_invocation = parent_invocation

def create_invoked_step(invoked_workflow, step, arguments, parent_invocation):
    """Indicate that a step is about to be executed. Return an ID."""

    raise NotImplementedError()

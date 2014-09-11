import mr.handlers.scope


class WorkflowScopeFactory(mr.handlers.scope.WorkflowScopeFactory):
    """Factory that renders a HandlerScopeFactory for a given workflow."""

    def get_handler_scope_factory(self, workflow):
        return _HandlerScopeFactory(workflow)


class _HandlerScopeFactory(mr.handlers.scope.HandlerScopeFactory):
    """Factory class that renders a scope dictionary for a given handler in a 
    particular workflow.
    """

    def __init__(self, workflow):
        self.__workflow = workflow

    def get_scope(self, handler):
        return { 'test_scope_val': 99 }

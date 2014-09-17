"""This file describes types that will get injected into the scope of handlers.
"""


class MrConfigure(object):
    pass


class MrConfigureToMap(MrConfigure):
    def __init__(self, next_step_name):
        self.__next_step_name = next_step_name

    @property
    def next_step_name(self):
        return self.__next_step_name


class MrConfigureToReturn(MrConfigure):
    pass

SCOPE_INJECTED_TYPES = {
    'MrConfigureToMap': MrConfigureToMap,
    'MrConfigureToReturn': MrConfigureToReturn,
}


class WorkflowScopeFactory(object):
    """Factory that renders a HandlerScopeFactory for a given workflow."""

    def get_handler_scope_factory(self, workflow):
        raise NotImplementedError()


class HandlerScopeFactory(object):
    """Factory class that renders a scope dictionary for a given handler in a 
    particular workflow.
    """

    def get_scope(self, handler_definition):
        raise NotImplementedError()

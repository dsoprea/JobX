import mr.handlers


class StepHandlersBase(mr.handlers.HandlersBase):
    """Manages the code for each step handler."""

    def get_handler_list_version(self):
        """Get an opaque string that describes the set of handlers and their 
        step classifications.
        """

        raise NotImplementedError()

    def get_handler_classifications(self):
        """Get a list of handlers and the step classifications that they 
        represent.
        """

        raise NotImplementedError()

    def get_code_handler(self, handler_name):
        """Return the code for the given handler."""

        raise NotImplementedError()

    def get_code_all_handlers(self):
        """Return the code for each and every current handler."""

        raise NotImplementedError()

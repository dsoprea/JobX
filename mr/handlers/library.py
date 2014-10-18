import mr.models.kv.handler
import mr.handlers.general


class LibraryAdapter(object):
    """Describes a class that knows where we're storing handlers for use during
    jobs, and how to put them there. This is used to access the handlers while
    processing steps.
    """

    def list_handlers(self):
        """Enumerate the handlers as (name, version)."""

        raise NotImplementedError()        

    def get_handler(self, handler_name):
        """Return a handler-definition object."""

        raise NotImplementedError()

    def create_handler(self, handler_definition):
        """Store the given new handler."""

        raise NotImplementedError()

    def update_handler(self, handler_definition):
        """Update the given existing handler."""

        raise NotImplementedError()

    def delete_handler(self, handler_name):
        """Delete the given handler."""

        raise NotImplementedError()

    def set_handlers_state(self, state):
        """Receive a unique state string that'll change when the sourcecode 
        does.
        """

        raise NotImplementedError()

    def get_handlers_state(self):
        """Returns the current state string."""

        raise NotImplementedError()


class KvLibraryAdapter(LibraryAdapter):
    """Manages handlers that are stored in the KV."""

    def __init__(self, workflow):
        self.__workflow = workflow
        self.__handlers = {}
        self.__state = None

    def list_handlers(self):
        """Enumerate the handlers as (name, version)."""

        return ((name,
                 [name for (name, cls_) in handler_info.argument_spec],
                 handler_info.version)
                for (name, handler_info)
                in self.__handlers.iteritems())

    def get_handler(self, handler_name):
        """Return a handler-definition object."""

        return self.__handlers[handler_name]

    def create_handler(self, hd):
        """Store the given new handler."""

        self.__handlers[hd.name] = hd

    def update_handler(self, hd):
        """Update the given existing handler."""

        self.__handlers[hd.name] = hd

    def delete_handler(self, handler_name):
        """Delete the given handler."""

        del self.__handlers[handler_name]

    def set_handlers_state(self, state):
        """Receive a unique state string that'll change when the sourcecode 
        does.
        """

        self.__state = state

    def get_handlers_state(self):
        """Returns the current state string."""

        return self.__state

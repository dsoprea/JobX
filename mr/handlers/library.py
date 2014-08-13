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

    def list_handlers(self):
        """Enumerate the handlers as (name, version)."""

        for h in mr.models.kv.handler.Handler.list():
            yield (
                h.handler_name, 
                [arg_info[0] for arg_info in h.argument_spec], 
                h.version
            )

    def get_handler(self, handler_name):
        """Return a handler-definition object."""

        handler = mr.models.kv.handler.get(self.__workflow, handler_name)

        return mr.handlers.general.HANDLER_DEFINITION_CLS(
                name=handler.handler_name,
                version=handler.version,
                description=handler.description,
                source_code=handler.source_code,
                argument_spec=handler.argument_spec,
                source_type=handler.source_type)

    def create_handler(self, hd):
        """Store the given new handler."""

        h = mr.models.kv.handler.Handler(
                workflow=self.__workflow, 
                name=hd.name, 
                description=hd.description,
                source_code=hd.source_code)

        h.save()

    def update_handler(self, hd):
        """Update the given existing handler."""

        h = mr.models.kv.handler.get(self.__workflow, hd.name)

        h.description = hd.description
        h.source_code = hd.source_code
        h.argument_spec = hd.argument_spec
        h.source_type = hd.source_type

        h.save()

    def delete_handler(self, handler_name):
        """Delete the given handler."""

        h = mr.models.kv.handler.get(self.__workflow, handler_name)
        h.delete(self.__workflow, handler_name)

    def set_handlers_state(self, state):
        """Receive a unique state string that'll change when the sourcecode 
        does.
        """

        self.__workflow.handlers_state = state
        self.__workflow.save()

    def get_handlers_state(self):
        """Returns the current state string."""

        # Make sure that we're in sync, since any host in the cluster can 
        # affect this.
        self.__workflow.refresh()

        return self.__workflow.handlers_state

import logging
import os
import fnmatch
import hashlib
import json

import mr.handlers.general
import mr.models.kv.handler

_logger = logging.getLogger(__name__)


class SourceAdapter(object):
    """Describes a class that knows how to get code for handlers. This is used 
    only when updating handlers used for steps.
    """

    def list_handlers(self):
        """Get a list of (handler names, version-strings)."""

        raise NotImplementedError()

    def get_handler(self, name):
        """Get the given definition."""

        raise NotImplementedError()

    def get_definition_from_obj(self, handler):
        """Return a handler-definition from the object."""

        raise NotImplementedError()

    def get_handlers_state(self, handlers=None):
        """Return a string that will change any time one of the handlers 
        changes.
        """

        raise NotImplementedError()


class KvSourceAdapter(SourceAdapter):
    """Retrieves sourcecode for handlers from the local filesystem."""

    def __init__(self, workflow):
        self.__workflow = workflow

    def __enumerate_handlers(self):
        workflow_name = self.__workflow.workflow_name
        handlers = mr.models.kv.handler.Handler.list(workflow_name)

        try:
            for handler in handlers:
                yield (handler.handler_name, handler.version)
        except KeyError:
            _logger.warning("No handlers are defined.")

    def list_handlers(self):
        return self.__enumerate_handlers()

    def get_definition_from_obj(self, handler):
        return mr.handlers.general.HANDLER_DEFINITION_CLS(
                name=handler.handler_name,
                version=handler.version,
                description=handler.description,
                source_code=handler.source_code,
                argument_spec=handler.argument_spec,
                source_type=handler.source_type,
                cast_arguments=handler.cast_arguments)

    def get_handler(self, name):
        handler = mr.models.kv.handler.get(self.__workflow, name)
        return self.get_definition_from_obj(handler)

    def get_handlers_state(self):
        self.__workflow.refresh()
        return self.__workflow.handlers_state

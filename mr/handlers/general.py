import datetime
import random
import hashlib
import os
import fnmatch
import hashlib
import logging
import collections
import json

import mr.config.handler
import mr.models.kv.handler
import mr.models.kv.workflow

_logger = logging.getLogger(__name__)

HANDLER_DEFINITION_CLS = collections.namedtuple(
                            'HandlerDefinition', 
                            ['name', 
                             'version', 
                             'description', 
                             'source_code',
                             'argument_spec',
                             'source_type'])


class HandlerFormatException(Exception):
    pass


class Handlers(object):
    """The base-class of our handler code libraries."""

    def __init__(self, source, library):
        self.__source = source
        self.__library = library
        self.__compiled = {}

        self.__update_handlers()

    def __update_handlers(self):
        """Push all of the current handlers, and their state string."""

        # Determine whether the code has changed.

        handler_state = self.__source.get_handlers_state()

        if handler_state == self.__library.get_handlers_state():
            _logger.debug("No update necessary.")
            return

        # The code HAS changed. Determine what has changed and how.

        stored_handlers = self.__library.list_handlers()
        stored_handler_versions = set(stored_handlers)
        stored_handler_names = [n for (n, v) in stored_handlers]

        source_handlers = list(self.__source.list_handlers())
        source_handler_versions = set(source_handlers)
        source_handler_names = [n for (n, v) in source_handlers]

        delta_handlers = source_handler_versions - stored_handler_versions
        delta_handler_names = set([name for (n, v) in delta_handlers])

        new_handler_names = source_handler_names - stored_handler_names
        deleted_handler_names = stored_handler_names - source_handler_names
        updated_handler_names = delta_handler_names - new_handler_names

        # Apply the changes.

        self.__library.set_handlers_state(handler_state)

        _logger.info("Updating handlers: NEW=(%d) UPDATED=(%d) DELETED=(%d)",
                     new_handler_names, updated_handler_names,
                     deleted_handler_names)

        for new_handler_name in new_handler_names:
            try:
                hd = self.__source.get_handler(new_handler_name)
            except HandlerFormatException:
                _logger.exception("Skipping new but unloadable handler from "
                                  "source: [%s]", new_handler_name)
                continue

            self.__library.create_handler(hd)

        for updated_handler_name in updated_handler_names:
            del self.__compiled[updated_handler_name]

            try:
                hd = self.__source.get_handler(updated_handler_name)
            except HandlerFormatException:
                _logger.exception("Skipping updated but unloadable handler "
                                  "from source: [%s]", new_handler_name)
                continue

            self.__library.update_handler(hd)

        for deleted_handler_name in deleted_handler_names:
            self.__library.delete_handler(deleted_handler_name)
            del self.__compiled[deleted_handler_name]

        self.__compile_handlers()

    def __compile_handlers(self):
        for name, version in self.__library.list_handlers():
            if name in self.__compiled:
                continue

            _logger.info("Compiling handler: [%s]", name)

            hd = self.__library.get_handler(name)

            # Split the code-body into lines.
            code_lines = hd.source_code.replace('\r', '').split('\n')

            self.__compiled[name] = self.__compile(code_lines)
        else:
            _logger.warning("No code has to be recompiled, even though the "
                            "handlers were supposed to have changed.")

    def __compile(self, code_lines):
        """Compile the body of code (split into lines). We do this by wrapping 
        it in another function, and then grabbing that function from scope. 
        This way, it can actually return a value in the most natural way.
        """

        id_ = hashlib.sha1(random.random()).hexdigest()
        code = "def " + id_ + "(args):\n" + \
               "\n".join(('  ' + line) for line in code_lines) + "\n"

        c = compile(code, name, 'exec')
        locals_ = {}
        exec(c, globals, locals_)

        return locals_[id_]

    def run_handler(self, name, arguments, **scope_references):
        locals_ = {}
        locals_.update(arguments)
        locals_.update(scope_references)

        exec(self.__compiled[name], globals, locals_)

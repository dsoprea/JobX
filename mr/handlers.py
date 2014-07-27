import datetime
import random
import hashlib
import os
import fnmatch
import hashlib
import logging

import mr.config.handler
import mr.models.kv.handler
import mr.models.kv.workflow

_logger = logging.getLogger(__name__)


class SourceAdapter(object):
    """Describes a class that knows how to get code for handlers. This is used 
    only when updating handlers used for steps.
    """

    def list_handlers(self):
        """Get a list of handlers' names and version-strings."""

        raise NotImplementedError()

    def get_handler_source(self, name):
        """Get the sourcecode and version string for the given handler."""

        raise NotImplementedError()

    def get_handlers_state(self):
        """Return a string that will change any time one of the handlers 
        changes.
        """

        raise NotImplementedError()


class FilesystemSourceAdapter(SourceAdapter):
    """Retrieves sourcecode for handlers from the local filesystem."""

    def __init__(self):
        if os.path.exists(deploy_ui.config.handler.SOURCE_PATH) is False:
            raise EnvironmentError("Source path does not exist: [%s]" % 
                                   (deploy_ui.config.handler.SOURCE_PATH,))

    def __enumerate_handlers(self):
        pattern = deploy_ui.config.handler.SOURCE_FILENAME_PATTERN
        source_path = deploy_ui.config.handler.SOURCE_PATH

        for filename in os.listdir(source_path):
            if fnmatch.fnmatch(filename, pattern) is False:
                continue

            filepath = os.path.join(source_path, filename)
            mtime = os.stat(filepath).st_mtime
            version = hashlib.md5(str(mtime)).hexdigest()

            (name, _) = os.path.splitext(filename)

            yield (name, version)

    def list_handlers(self):
        return self.__enumerate_handlers()

    def get_handler_source(self, name):
        replacements = {
            'name': name
        }

        filename = deploy_ui.config.handler.SOURCE_FILENAME_TEMPLATE % \
                   replacements

        filepath = os.path.join(deploy_ui.config.handler.SOURCE_PATH, filename)

        with open(filepath) as f:
            return f.read()

    def get_handlers_state(self):
        states = [version for (name, version) in self.__enumerate_handlers()]
        return hashlib.sha1(','.join(states)).hexdigest()


class LibraryAdapter(object):
    """Describes a class that knows where we're storing handlers for use during
    jobs, and how to put them there. This is used to access the handlers while
    processing steps.
    """

    def list_handlers(self):
        """Enumerate all of the stored handlers, and their versions."""

        raise NotImplementedError()        

    def update_handlers(self, source):
        """Push all of the current handlers, and their state string."""

        raise NotImplementedError()


class KvLibraryAdapter(LibraryAdapter):
    """Manages handlers that are stored in the KV."""

    def __init__(self, workflow):
        self.__workflow = workflow

    def list_handlers(self):
        raise NotImplementedError()

    def update_handlers(self, source):
        """Push all of the current handlers, and their state string."""

        handler_state = source.get_handlers_state()

        if self.__workflow.handlers_state == handler_state:
            _logger.debug("No update necessary.")
            return

        stored_handlers = self.list_handlers()
        stored_handler_versions = set(stored_handlers)
        stored_handler_names = [n for (n, v) in stored_handlers]

        source_handlers = source.list_handlers()
        source_handler_versions = set(source_handlers)
        source_handler_names = [n for (n, v) in source_handlers]

        delta_handlers = source_handler_versions - stored_handler_versions
        delta_handler_names = set([name for (n, v) in delta_handlers])

        new_handler_names = source_handler_names - stored_handler_names
        deleted_handler_names = stored_handler_names - source_handler_names
        updated_handler_names = delta_handler_names - new_handler_names

        _logger.info("Updating handlers: NEW=(%d) DELETED=(%d) UPDATED=(%d)",
                     new_handler_names, deleted_handler_names, 
                     updated_handler_names)

        mr.models.kv.handler.


# TODO(dustin): get_children_encoded(self, parent, identity)
        raise NotImplementedError()


class Handlers(object):
    """The base-class of our handler code libraries."""

    def __init__(self, workflow):
        self.__state = None

        self.__update_handlers()

    def run_handler(self, name, arguments, **scope_references):
        locals_ = {}
        locals_.update(arguments)
        locals_.update(scope_references)

        exec(self.__state[2][name], globals, locals_)

    def __compile(self, code_lines):
            id_ = hashlib.sha1(random.random()).hexdigest()
            code = "def " + id_ + "(args):\n" + \
                   "\n".join(('  ' + line) for line in code_lines) + "\n"

            c = compile(code, name, 'exec')
            locals_ = {}
            exec(c, globals, locals_)

            return locals_[id_]

    def __update_handlers(self):
        """Get a list of handlers and the classifications that they represent.
        """

# TODO(dustin): Load the handlers from etcd.

        sum_function = """\
return xrange(arg1)
"""

        handlers = {}

        def add_handler(name, code_lines):
            handlers[name] = self.__compile(code_lines)

        self.__state = (1, datetime.datetime.now(), handlers)

    @property
    def list_version(self):
        return self.__state[0]

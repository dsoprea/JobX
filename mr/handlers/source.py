import os
import fnmatch
import hashlib
import json

import mr.config.handler
import mr.handlers.general


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

    def get_handlers_state(self, handlers=None):
        """Return a string that will change any time one of the handlers 
        changes.
        """

        raise NotImplementedError()


class FilesystemSourceAdapter(SourceAdapter):
    """Retrieves sourcecode for handlers from the local filesystem."""

    def __init__(self, workflow):
        if os.path.exists(mr.config.handler.SOURCE_PATH) is False:
            raise EnvironmentError("Source path does not exist: [%s]" % 
                                   (mr.config.handler.SOURCE_PATH,))

        self.__workflow = workflow

    def __enumerate_handlers(self):
        pattern = mr.config.handler.SOURCE_FILENAME_PATTERN
        source_path = mr.config.handler.SOURCE_PATH

        for filename in os.listdir(source_path):
            if fnmatch.fnmatch(filename, pattern) is False:
                continue

            filepath = os.path.join(source_path, filename)

            with open(filepath) as f:
                source_code = f.read()

            version = hashlib.sha1(source_code).hexdigest()

            (name, _) = os.path.splitext(filename)

            yield (name, version)

    def list_handlers(self):
        return self.__enumerate_handlers()

    def get_handler(self, name):
        replacements = {
            'name': name
        }

        filename = mr.config.handler.SOURCE_FILENAME_TEMPLATE % \
                   replacements

        filepath = os.path.join(mr.config.handler.SOURCE_PATH, filename)

# TODO: If we maintain a stamp file that has the hash and an mtime synced to 
#       the handler file, we won't have to constantly recalculate the hash.
        with open(filepath) as f:
            source_code = f.read()

        meta_filepath = filepath + \
                        mr.config.handler.SOURCE_META_FILENAME_SUFFIX

        with open(meta_filepath) as f:
            meta = json.load(f)

        hd = mr.handlers.general.HANDLER_DEFINITION_CLS(
                name=name, 
                version=hashlib.sha1(source_code).hexdigest(), 
                description=meta['description'], 
                source_code=source_code, 
                argument_spec=meta['argument_spec'], 
                source_type=meta['source_type'])

        self.__validate_handler(hd)

        return hd

    def __validate_handler(self, hd):
        """Return a mr.handlers.general.HandlerFormatException if the fields 
        aren't correct.
        """
# TODO(dustin): Finish.
        pass

    def get_handlers_state(self, handlers=None):
        if handlers is None:
            handlers = self.__enumerate_handlers()

        states = [version for (name, version) in handlers]
        return hashlib.sha1(','.join(states)).hexdigest()

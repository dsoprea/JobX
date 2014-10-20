import logging
import json
import yaml
import hashlib
import os

import mr.models.kv.step
import mr.models.kv.handler
import mr.handlers.utility

# Documentation parsers
DP_YAML = 'yaml'
DP_JSON = 'json'

_logger = logging.getLogger(__name__)


class HandlerSync(object):
    def __init__(self, workflow, root_path, simulate_only=False, 
                 force_update=False):
        self.__workflow = workflow
        self.__root_path = root_path
        self.__simulate_only = simulate_only
        self.__force_update = force_update

    def __get_recursive_file_gen(self):
        for (path, dir_entries, file_entries) in os.walk(self.__root_path):
            rel_path = path[len(self.__root_path) + 1:]

            if rel_path != '':
                prefix = rel_path.replace('/', '_') + '_'
            else:
                prefix = ''

            for filename in file_entries:
                if filename[0] == '_':
                    _logger.info("[%s]: Ignoring.", filename)
                    continue

                filepath = os.path.join(path, filename)
                
                pivot = filename.rfind('.')
                if pivot == -1:
                    continue

                handler_name = prefix + filename[:pivot]
                extension = filename[pivot + 1:]

                rel_filepath = os.path.join(rel_path, filename)
                yield (filepath, rel_filepath, extension, handler_name)

    def __scan_files(self, path):
        _logger.info("Scanning files (and adding/updating handlers):")
        handlers_s = set()
        extension_map = mr.config.handler.CODE_EXTENSION_MAP
        for (filepath, rel_filepath, extension, handler_name) \
            in self.__get_recursive_file_gen():
                try:
                    source_type = extension_map[extension]
                except KeyError:
                    _logger.warning("[%s]: File does not look it's a source-"
                                    "file [that we can process]: [%s]",
                                    rel_filepath, extension)

                    continue

                self.__check_handler(
                    filepath, 
                    handler_name, 
                    source_type)

                handlers_s.add(handler_name)

        handlers_to_delete = [handler 
                              for handler 
                              in mr.models.kv.handler.Handler.list(
                                    self.__workflow.workflow_name)
                              if handler.handler_name not in handlers_s]

        if handlers_to_delete:
            # Tabulate the handlers being used by steps.

            _logger.info("Identifying handlers currently being used.")

            used_handlers = {}
            for step in mr.models.kv.step.Step.list(
                            self.__workflow.workflow_name):
                try:
                    used_handlers[step.map_handler_name].append(step.step_name)
                except KeyError:
                    used_handlers[step.map_handler_name] = [step.step_name]

                if step.reduce_handler_name is not None:
                    try:
                        handler_steps = used_handlers[step.reduce_handler_name]
                    except KeyError:
                        used_handlers[step.reduce_handler_name] = \
                            [step.step_name]
                    else:
                        handler_steps.append(step.step_name)

                if step.combine_handler_name is not None:
                    try:
                        handler_steps = used_handlers[
                                            step.combine_handler_name]
                    except KeyError:
                        used_handlers[step.combine_handler_name] = \
                            [step.step_name]
                    else:
                        handler_steps.append(step.step_name)

            # Remove handlers that we didn't find source-code for.

            _logger.info("Removing handlers that no longer have source-code "
                         "files:")

            for handler in handlers_to_delete:
                # Verify that no steps implement this handler.

                if handler.handler_name in used_handlers:
                    # *Prints a list of referencing steps.
                    _logger.info("[%s]: Did not have source-code file, but we "
                                 "can't remove it since it is used by: %s",
                                 handler.handler_name, 
                                 used_handlers[handler.handler_name])
                else:
                    _logger.info("[%s]: Removing (not referenced by anything, "
                                 "either).", handler.handler_name)

                    if self.__simulate_only is False:
                        # Nothing uses it. Delete it.
                        handler.delete()
        else:
            _logger.info("No handlers need to be removed.")

    def __parse_doc_string(self, doc_string):
        # Normalize.
        distilled = doc_string.replace('\r', '').strip()

        lines = distilled.split('\n')
        pivot_line = None
        type_ = DP_YAML

        for i, line in enumerate(lines):
            line_stripped = line.strip()
            if line_stripped[:2] == '**':
                pivot_line = i
                type_raw = line_stripped[2:]
                if type_raw:
                    type_ = type_raw

                break
        else:
            raise ValueError("No description could be isolated.")

        type_ = type_.lower()

        description = '\n'.join(lines[:pivot_line])
        meta_raw = '\n'.join(lines[pivot_line + 1:])

        if type_ == DP_YAML:
            meta = yaml.load(meta_raw)
        elif type_ == DP_JSON:
            meta = json.loads(meta_raw)
        else:
            raise ValueError("Parser for description is invalid: [%s]" % 
                             (type_,))

        return (description, meta)

    def __check_handler(self, filepath, name, source_type):
        with open(filepath) as f:
            source_code = f.read()

        processor = mr.handlers.utility.get_processor(source_type)

        # We won't have the argument list until we parse the documentation. It 
        # doesn't have to be callable, we just need the documentation.
        (doc_string, f) = processor.compile(name, [], source_code)

        try:
            (description, meta) = self.__parse_doc_string(doc_string)
        except:
            _logger.exception("There was a problem with the documentation for "
                              "[%s]. TYPE=[%s]\n\nDoc String:\n\n%s\n" % 
                              (name, source_type, doc_string.strip()))
            raise

        # The arg-spec is expressed as a dictionary in order to be more 
        # readable. Convert it to a list of tuples.
        meta['argument_spec'] = [(a['name'], a['type']) 
                                 for a 
                                 in meta['argument_spec']]

        required_fields_s = set(mr.config.handler.REQUIRED_META_FIELDS)
        available_fields_s = set(meta.keys())

        if required_fields_s != available_fields_s:
            raise ValueError("[%s] Handler has invalid/missing meta-fields: "
                             "[%s] != [%s]" % 
                             (name, required_fields_s, available_fields_s))

        version = hashlib.sha1(doc_string + source_code).hexdigest()

        try:
            handler = mr.models.kv.handler.get(self.__workflow, name)
        except KeyError:
            self.__create_handler(
                name, 
                meta, 
                description, 
                source_type, 
                source_code, 
                version)
        else:
            if handler.version != version or self.__force_update:
                self.__update_handler(
                    handler, 
                    meta, 
                    description,
                    source_type,
                    source_code, 
                    version)
            else:
                _logger.info("[%s]: Not changed.", name)

    def __validate_handler(self, handler):
        """See if we can naively compile the code."""

        processor = mr.handlers.utility.get_processor(handler.source_type)

        _logger.debug("Attempting to compile routine [%s] with processor "
                      "[%s].",
                      handler.handler_name, processor.__class__.__name__)

        arg_names = [k for (k, v) in handler.argument_spec]

        processor.compile(handler.handler_name, arg_names, handler.source_code)

    def __get_extra_arguments_spec(self, handler_type):
        """Return the arguments that we will secretly always send in, in 
        addition to the arguments specified.
        """

        return [
            ('ctx', None),
        ]

    def __create_handler(self, name, meta, description, source_type, 
                         source_code, version):
        _logger.info("[%s]: Creating.", name)

# TODO(dustin): We might also want to require "test" arguments and a test 
#               result, for unit-tests.

        handler_type = meta['handler_type']

        expected_arguments = meta['argument_spec']
        extra_argument_spec = self.__get_extra_arguments_spec(handler_type)
        expected_arguments += extra_argument_spec

        handler = mr.models.kv.handler.Handler(
                    workflow_name=self.__workflow.workflow_name,
                    handler_name=name,
                    description=description,
                    argument_spec=expected_arguments,
                    source_type=source_type,
                    source_code=source_code,
                    version=version,
                    handler_type=handler_type,
                    required_capability=meta['required_capability'])

        self.__validate_handler(handler)

        if self.__simulate_only is False:
            handler.save()

    def __update_handler(self, handler, meta, description, source_type, 
                         source_code, version):
        _logger.info("[%s]: Updating.", handler.handler_name)

# TODO(dustin): We might also want to require "test" arguments and a test 
#               result, for unit-tests.

        handler_type = meta['handler_type']

        expected_arguments = meta['argument_spec']
        extra_argument_spec = self.__get_extra_arguments_spec(handler_type)
        expected_arguments += extra_argument_spec

        handler.description = description
        handler.argument_spec = expected_arguments
        handler.source_type = source_type
        handler.source_code = source_code
        handler.version = version
        handler.handler_type = handler_type
        handler.required_capability = meta['required_capability']

        self.__validate_handler(handler)

        if self.__simulate_only is False:
            handler.save()

    def run(self):
        if self.__simulate_only is True:
            _logger.info("SIMULATION ONLY!")

        self.__scan_files(self.__root_path)

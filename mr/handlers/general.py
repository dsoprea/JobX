import random
import hashlib
import os
import logging
import collections
import time
import threading
import functools

import mr.config.log
import mr.models.kv.handler
import mr.models.kv.workflow
import mr.handlers.utility
import mr.handlers.scope
import mr.fs.general
import mr.log

_PATH_SEP = '/'

HANDLER_DEFINITION_CLS = collections.namedtuple(
                            'HandlerDefinition', 
                            ['name', 
                             'version', 
                             'description', 
                             'source_code',
                             'argument_spec',
                             'source_type',
                             'cast_arguments'])

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


class HandlerFormatException(Exception):
    pass


class _WrappedLog(object):
    """The arguments to the HTTP logger will not be filled-in by the time 
    they're sent out, but because of the way they're sent, it's tough to do in 
    the HTTP receiver. So, we do it upfront, here.
    """

    def __init__(self, logger):
        self.__logger = logger

    def __log(self, type_, message, *args):
        message = message % args
        return getattr(self.__logger, type_)(message)

    def __getattr__(self, name):
        return functools.partial(self.__log, name)


class Handlers(object):
    """The base-class of our handler code libraries."""

    def __init__(self, workflow, source, library, handler_scope_factory=None):
        assert issubclass(
                handler_scope_factory.__class__, 
                mr.handlers.scope.HandlerScopeFactory) is True or \
               handler_scope_factory is None

        self.__workflow = workflow
        self.__source = source
        self.__library = library
        self.__hsf = handler_scope_factory

        self.__compiled = {}

        self.__ucl_exit_ev = None
        self.__ucl_t = None

        self.__schedule_update_check()

    def __del__(self):
        if self.__ucl_t is not None:
            self.__ucl_exit_ev.set()
            self.__ucl_t.join()

    def __schedule_update_check(self):
        """Schedule a continuous check of the handler states, so that we'll 
        update them when they change. Note that the default/original 
        implementation of our handlers' source interface checks the KV, not the
        filesystem, since the filesystem is only a concern of the script that 
        loads them into the KV. A filesystem-auditing tool would have to be 
        created in order to update the KV when the files change.
        """

        _logger.info("Scheduling the handler update-check.")

        self.__ucl_exit_ev = threading.Event()

        self.__ucl_t = threading.Thread(target=self.__update_check_loop)
        self.__ucl_t.start()

    def __update_check_loop(self):
        while self.__ucl_exit_ev.is_set() is False:
            _logger.debug("Commencing update check.")
            self.__update_handlers()
            time.sleep(mr.config.handler.HANDLER_UPDATE_INTERVAL_S)

    def __update_handlers(self):
        """Push all of the current handlers, and their state string."""

        _logger.debug("Updating handlers.")

        # Determine whether the code has changed.

        source_state = self.__source.get_handlers_state()
        library_state = self.__library.get_handlers_state()

        _logger.debug("Handler states: SOURCE=[%s] LIBRARY=[%s]",
                      source_state, library_state)

        if source_state == library_state:
            _logger.debug("No update necessary.")
            return

        # The code HAS changed. Determine what has changed and how.

        stored_handlers = list(self.__library.list_handlers())
        stored_handler_versions_s = set([(n, v) for (n, a, v) in stored_handlers])
        stored_handler_names_s = set([n for (n, a, v) in stored_handlers])

        source_handlers = list(self.__source.list_handlers())
        source_handler_versions_s = set(source_handlers)
        source_handler_names_s = set([n for (n, v) in source_handlers])

        delta_handlers_s = source_handler_versions_s - stored_handler_versions_s
        delta_handler_names_s = set([n for (n, v) in delta_handlers_s])

        new_handler_names_s = source_handler_names_s - stored_handler_names_s
        deleted_handler_names_s = stored_handler_names_s - source_handler_names_s
        updated_handler_names_s = delta_handler_names_s - new_handler_names_s

        # Apply the changes.

        self.__library.set_handlers_state(source_state)

        message = "Applying handler updates: NEW=(%d) UPDATED=(%d) " \
                  "DELETED=(%d)" % \
                  (len(new_handler_names_s), len(updated_handler_names_s),
                   len(deleted_handler_names_s))

        _logger.info(message)

        # If the handlers have been changed, send a notification.
        if len(stored_handlers) > 0:
            notify = mr.log.get_notify()
            notify.info(message)

        for new_handler_name in new_handler_names_s:
            try:
                hd = self.__source.get_handler(new_handler_name)
            except HandlerFormatException:
                _logger.exception("Skipping new but unloadable handler from "
                                  "source: [%s]", new_handler_name)
                continue

            self.__library.create_handler(hd)

        for updated_handler_name in updated_handler_names_s:
            del self.__compiled[updated_handler_name]

            try:
                hd = self.__source.get_handler(updated_handler_name)
            except HandlerFormatException:
                _logger.exception("Skipping updated but unloadable handler "
                                  "from source: [%s]", new_handler_name)
                continue

            self.__library.update_handler(hd)

        for deleted_handler_name in deleted_handler_names_s:
            self.__library.delete_handler(deleted_handler_name)
            del self.__compiled[deleted_handler_name]

        self.__compile_handlers()

    def __get_scope_objects(self, hd):
        fs = mr.fs.general.get_fs(self.__workflow)

        def path_join(*args):
            return _PATH_SEP.join(args)

        handler_log = logging.getLogger('MR_HANDLER')
        raw_log = handler_log.getChild('RAW').getChild(hd.name)

        scope = {
            'FS': fs,
            'SEP': _PATH_SEP,
            'JOIN': path_join,
            'LOG': raw_log,
            'NOTIFY': mr.log.get_notify(),
            '__name__': 'handler(' + hd.name + ')',
        }

        if mr.config.log.DO_HOOK_EMAIL is True:
            scope['EMAIL'] = handler_log.getChild('EMAIL').getChild(hd.name)
            
        if mr.config.log.DO_HOOK_HTTP is True:
            handler_http_logger = handler_log.getChild('HTTP').getChild(hd.name)
            scope['HTTP'] = _WrappedLog(handler_http_logger)

        return scope

    def __compile_handler(self, hd, arg_names, scope=None):
        if scope is None:
            scope = {}

        scope.update(mr.handlers.scope.SCOPE_INJECTED_TYPES)
        scope.update(self.__get_scope_objects(hd))

        processor = mr.handlers.utility.get_processor(hd.source_type)

        handler_scope = scope

        if self.__hsf is not None:
            handler_scope.update(self.__hsf.get_scope(hd))

        (meta, compiled) = processor.compile(
                            hd.name, 
                            arg_names, 
                            hd.source_code, 
                            scope=scope)
 
        return (meta, compiled)

    def __compile_handlers(self):
        scope = {}
        scope.update(mr.handlers.scope.SCOPE_INJECTED_TYPES)

        handler_count = 0
        for name, arg_names, version in self.__library.list_handlers():
            handler_count += 1

            if name in self.__compiled:
                continue

            _logger.info("Compiling handler: [%s]", name)

            hd = self.__library.get_handler(name)
            (meta, compiled) = self.__compile_handler(hd, arg_names, scope=scope)

            self.__compiled[name] = compiled

        if handler_count == 0:
            _logger.warning("No handlers were presented by the library. No "
                            "code was compiled.")

    def run_handler(self, name, arguments_dict):
        hd = self.__library.get_handler(name)

        arguments_list = [v for (k, v) in hd.cast_arguments(arguments_dict)]

        try:
            compiled = self.__compiled[name]
        except KeyError:
            _logger.exception("Handler [%s] is not registered.", name)
            raise

        processor = mr.handlers.utility.get_processor(hd.source_type)
        return processor.run(compiled, arguments_list)

def update_workflow_handle_state(workflow_name):
    _logger.debug("Updating workflow with new handlers state.")

    def calculate_state():
        # Stamp a hash that represents -all- of the handlers' states on the 
        # workflow.
        _logger.debug("Calculating handlers state.")

        versions = (str(h.version) 
                    for h 
                    in mr.models.kv.handler.Handler.list(workflow_name))

        hash_ = hashlib.sha1(','.join(versions)).hexdigest()
        _logger.debug("Calculated handlers-state: [%s]", hash_)

        return hash_

    workflow = mr.models.kv.workflow.get(workflow_name)

    def get_cb():
        workflow.refresh()
        return workflow

    def set_cb(obj):
        obj.handlers_state = calculate_state()

    workflow.__class__.atomic_update(get_cb, set_cb)
# TODO(dustin): Verify that the workflow record reflects the latest state.
    _logger.debug("Workflow handlers state is now: [%s]", 
                  workflow.handlers_state)

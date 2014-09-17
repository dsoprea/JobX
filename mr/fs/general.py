import logging
import os.path
import contextlib
import collections

import mr.config.fs
import mr.utility

_logger = logging.getLogger(__name__)

_SEP = '/'
_DOT = '.'

_PATH_ERROR_MESSAGE = "Path must be relative and not higher than the current "\
                      "directory."

_FROM_PATH_ERROR_MESSAGE = "'from' path must be relative and not higher than "\
                           "the current directory."

_TO_PATH_ERROR_MESSAGE = "'to' path must be relative and not higher than the "\
                         "current directory."

# ctime is always/often None in Tahoe.
_STAT_CLS = collections.namedtuple(
                'EntryStat', 
                ('ctime', 'name', 'size', 'st_mode', 'type', 'uri'))

_FS = None


class _FilesystemWrapper(object):
    def __init__(self, fs, workflow):
        self.__fs = fs
        self.__workflow = workflow

        self.__path_prefix = _SEP + os.path.join(
                                        mr.config.fs.WORKING_DIRECTORY_NAME,
                                        workflow.workflow_name)

        if self.__exists(self.__path_prefix) is False:
            self.__mkdir(self.__path_prefix, recursive=True)

    def __str__(self):
        return '<Filesystem(' + self.__resource.__class__.__name__ + ')>'

    def __mkdir(self, path, *args, **kwargs):
        self.__fs.makedir(path, *args, **kwargs)

    def mkdir(self, rel_path, *args, **kwargs):

        rel_path = os.path.normpath(rel_path)

        assert rel_path
        assert rel_path[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        path = os.path.join(self.__path_prefix, rel_path)
        self.__mkdir(path, *args, **kwargs)

    def ls(self, rel_path, *args, **kwargs):
        """Enumerate file entries and meta."""

        rel_path = os.path.normpath(rel_path)

        assert rel_path
        assert rel_path[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        path = os.path.join(self.__path_prefix, rel_path)
        entries_raw_gen = self.__fs.ilistdirinfo(path, *args, **kwargs)
        entries_gen = ((name, _STAT_CLS(**info)) 
                       for (name, info) 
                       in entries_raw_gen)

        return entries_gen

    def cp(self, from_rel_filepath, to_rel_filepath, *args, **kwargs):
        """Copy file."""

        from_rel_filepath = os.path.normpath(from_rel_filepath)
        to_rel_filepath = os.path.normpath(to_rel_filepath)

        assert from_rel_filepath
        assert to_rel_filepath
        assert from_rel_filepath[0] not in (_SEP, _DOT), \
               _FROM_PATH_ERROR_MESSAGE
        assert to_rel_filepath[0] not in (_SEP, _DOT), \
               _TO_PATH_ERROR_MESSAGE

        from_filepath = os.path.join(self.__path_prefix, from_rel_filepath)
        to_filepath = os.path.join(self.__path_prefix, to_rel_filepath)

        self.__fs.copy(from_filepath, to_filepath, *args, **kwargs)

    def cpr(self, from_rel_path, to_rel_path, *args, **kwargs):
        """Copy directory."""

        from_rel_path = os.path.normpath(from_rel_path)
        to_rel_path = os.path.normpath(to_rel_path)

        assert from_rel_path
        assert to_rel_path
        assert from_rel_path[0] not in (_SEP, _DOT), _FROM_PATH_ERROR_MESSAGE
        assert to_rel_path[0] not in (_SEP, _DOT), _TO_PATH_ERROR_MESSAGE

        from_path = os.path.join(self.__path_prefix, from_rel_path)
        to_path = os.path.join(self.__path_prefix, to_rel_path)

        self.__fs.copydir(from_path, to_path, *args, **kwargs)

    def rmdir(self, rel_path, *args, **kwargs):
        """Remove a file or directory."""

        rel_path = os.path.normpath(rel_path)

        assert rel_path
        assert rel_path[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        path = os.path.join(self.__path_prefix, rel_path)
        return self.__fs.removedir(path, *args, **kwargs)

    def rm(self, rel_filepath, *args, **kwargs):
        """Remove a file or directory."""

        rel_filepath = os.path.normpath(rel_filepath)

        assert rel_filepath
        assert rel_filepath[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        filepath = os.path.join(self.__path_prefix, rel_filepath)
        return self.__fs.remove(filepath, *args, **kwargs)

    def mv(self, from_rel_file_or_path, to_rel_file_or_path, *args, **kwargs):
        """Move a file."""

        from_rel_file_or_path = os.path.normpath(from_rel_file_or_path)
        to_rel_file_or_path = os.path.normpath(to_rel_file_or_path)

        assert from_rel_file_or_path
        assert to_rel_file_or_path
        assert from_rel_file_or_path[0] not in (_SEP, _DOT), \
               _FROM_PATH_ERROR_MESSAGE
        assert to_rel_file_or_path[0] not in (_SEP, _DOT), \
               _TO_PATH_ERROR_MESSAGE

        from_file_or_path = os.path.join(
                                self.__path_prefix, 
                                from_rel_file_or_path)
        
        to_file_or_path = os.path.join(
                            self.__path_prefix, 
                            to_rel_file_or_path)

        return self.__fs.move(
                from_file_or_path, 
                to_file_or_path, 
                *args, 
                **kwargs)

    def get_stat(self, rel_file_or_path, *args, **kwargs):

        rel_file_or_path = os.path.normpath(rel_file_or_path)

        assert rel_file_or_path
        assert rel_file_or_path[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        file_or_path = os.path.join(self.__path_prefix, rel_file_or_path)
        return self.__fs.getinfo(file_or_path, *args, **kwargs)

    def get_filesize(self, rel_filepath, *args, **kwargs):

        rel_filepath = os.path.normpath(rel_filepath)

        assert rel_filepath
        assert rel_filepath[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        filepath = os.path.join(self.__path_prefix, rel_filepath)
        return self.__fs.getsize(filepath, *args, **kwargs)

    def __exists(self, file_or_path, *args, **kwargs):

        return self.__fs.exists(file_or_path, *args, **kwargs)

    def exists(self, rel_file_or_path, *args, **kwargs):

        rel_file_or_path = os.path.normpath(rel_file_or_path)

        assert rel_file_or_path
        assert rel_file_or_path[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        file_or_path = os.path.join(self.__path_prefix, rel_file_or_path)
        return self.__exists(file_or_path, *args, **kwargs)

    def isfile(self, rel_file_or_path, *args, **kwargs):

        rel_file_or_path = os.path.normpath(rel_file_or_path)

        assert rel_file_or_path
        assert rel_file_or_path[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        file_or_path = os.path.join(self.__path_prefix, rel_file_or_path)
        return self.__fs.isfile(file_or_path, **kwargs)

    def isdir(self, rel_file_or_path, *args, **kwargs):

        rel_file_or_path = os.path.normpath(rel_file_or_path)

        assert rel_file_or_path
        assert rel_file_or_path[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        file_or_path = os.path.join(self.__path_prefix, rel_file_or_path)
        return self.__fs.isdir(file_or_path, *args, **kwargs)

    @contextlib.contextmanager
    def open(self, rel_filepath, *args, **kwargs):

        rel_filepath = os.path.normpath(rel_filepath)

        assert rel_filepath
        assert rel_filepath[0] not in (_SEP, _DOT), _PATH_ERROR_MESSAGE

        filepath = os.path.join(self.__path_prefix, rel_filepath)

        f = self.__fs.open(filepath, *args, **kwargs)
        yield f
        f.close()

def get_fs(workflow):
    global _FS

    fs_factory_fq_class = mr.config.fs.FS_FACTORY_FQ_CLASS
    if fs_factory_fq_class is not None:
        if _FS is None:
            _logger.info("Connecting filesystem with factory: [%s]", 
                         fs_factory_fq_class)

            fs_factory_cls = mr.utility.load_cls_from_string(
                                fs_factory_fq_class)

            fs_factory = fs_factory_cls()
            _FS_SPECIFIC = fs_factory.get_instance()
            _FS = _FilesystemWrapper(_FS_SPECIFIC, workflow)
    else:
        _logger.debug("No filesystem factory has been configured.")

    return _FS

import logging

import mr.config.fs
import mr.utility

_logger = logging.getLogger(__name__)

_FS = None

# TODO(dustin): We need to wrap the filesystem resource so that we can 
#               constrain the directories that can be affect (by workflow, for 
#               example).

def get_fs():
    global _FS

    fs_factory_fq_class = mr.config.fs.FS_FACTORY_FQ_CLASS
    if fs_factory_fq_class is not None:
        if _FS is None:
            _logger.info("Connecting filesystem with factory: [%s]", 
                         fs_factory_fq_class)

            fs_factory_cls = mr.utility.load_cls_from_string(
                                fs_factory_fq_class)

            fs_factory = fs_factory_cls()
            _FS = fs_factory.get_instance()
    else:
        _logger.debug("No filesystem factory has been configured.")

    return _FS

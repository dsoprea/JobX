import os
import logging

import fs.contrib.tahoelafs

import mr.fs.backend.fs_factory

logging.getLogger('fs.tahoelafs').setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)


class TahoeFilesystemFactory(mr.fs.backend.fs_factory.FilesystemFactory):
    def get_instance(self):
        dir_uri = os.environ['MR_FS_TAHOE_DIR_URI']
        webapi_url = os.environ['MR_FS_TAHOE_WEBAPI_URL_PREFIX']
         
        _logger.info("Connecting Tahoe-LAFS at [%s]. DIR_URI=[%s]", 
                     webapi_url, dir_uri)

        return fs.contrib.tahoelafs.TahoeLAFS(dir_uri, webapi=webapi_url)

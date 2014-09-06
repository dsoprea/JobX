import logging
import logging.handlers
import os

import mr.config

#logging.getLogger().setLevel(logging.WARNING)
#logging.getLogger('mr').setLevel(logging.DEBUG)

_FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_FORMATTER = logging.Formatter(_FMT)

logger = logging.getLogger()#'mr')

ch = logging.StreamHandler()
ch.setFormatter(_FORMATTER)

if mr.config.IS_DEBUG is True:
    logger.setLevel(logging.DEBUG)
    ch.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)

logger.addHandler(ch)

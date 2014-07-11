import logging
import logging.handlers
import os

import mr.config

_FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_FORMATTER = logging.Formatter(_FMT)

logger = logging.getLogger()

ch = logging.StreamHandler()
ch.setFormatter(_FORMATTER)

if mr.config.IS_DEBUG is True:
    logger.setLevel(logging.DEBUG)
    ch.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)

logger.addHandler(ch)

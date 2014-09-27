import logging
import logging.handlers
import os

import mr.config

_FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_FORMATTER = logging.Formatter(_FMT)

logger = logging.getLogger()

sh = logging.StreamHandler()
sh.setFormatter(_FORMATTER)
logger.addHandler(sh)

sh2 = logging.handlers.SysLogHandler()
sh2.setFormatter(_FORMATTER)
logger.addHandler(sh2)

if mr.config.IS_DEBUG is True:
#    fh = logging.FileHandler('/tmp/mr.flow.log')
#    fh.setFormatter(_FORMATTER)
#
#    flow_logger = logging.getLogger('mr.job_engine.flow')
#    flow_logger.addHandler(fh)
#    flow_logger.setLevel(logging.DEBUG)

    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

is_handler_debug = bool(int(os.environ.get('MR_HANDLER_DEBUG', '0')))
if is_handler_debug is True:
    handler_logger = logging.getLogger('MR_HANDLER')

    fh = logging.FileHandler('/tmp/mr.handler.log')
    fh.setFormatter(_FORMATTER)
    handler_logger.addHandler(fh)

    handler_log_level = os.environ.get('MR_HANDLER_LOG_LEVEL', 'DEBUG')
    handler_logger.setLevel(getattr(logging, handler_log_level))

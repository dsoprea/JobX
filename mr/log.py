import logging

import mr.config.log

_logger = logging.getLogger(__name__)

_handler_logger = logging.getLogger('MR_HANDLER')

handler_logger_email = _handler_logger.getChild('EMAIL')
handler_logger_http = _handler_logger.getChild('HTTP')


class _ExceptionNotifyWrapper(object):
    def exception(self, *args, **kwargs):
        if mr.config.log.DO_HOOK_EMAIL is True or \
           mr.config.log.DO_HOOK_HTTP is True:
            if mr.config.log.DO_HOOK_EMAIL is True:
                mr.config.log.handler_logger_email.exception(*args, **kwargs)

            if mr.config.log.DO_HOOK_HTTP is True:
                mr.config.log.handler_logger_http.exception(*args, **kwargs)
        else:
            _logger.warning("Exception notifications aren't hooked.")
            _logger.exception(*args, **kwargs)


_notify = None
def get_notify():
    global _notify

    if _notify is None:
        _notify = _ExceptionNotifyWrapper()

    return _notify

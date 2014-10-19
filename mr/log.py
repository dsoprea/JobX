import logging
import traceback

import mr.config.log

_logger = logging.getLogger(__name__)

_handler_logger = logging.getLogger('MR_HANDLER')

handler_logger_email = _handler_logger.getChild('EMAIL')
handler_logger_http = _handler_logger.getChild('HTTP')


class _ExceptionNotifyWrapper(object):
    def exception(self, message, *args):
        message = message % args

        # The HTTPHandler won't transfer the traceback.
        message_with_exception = message + "\n\n" + traceback.format_exc()

        if mr.config.log.DO_HOOK_EMAIL is True or \
           mr.config.log.DO_HOOK_HTTP is True:
            is_success = False

            if mr.config.log.DO_HOOK_EMAIL is True:
                try:
                    mr.config.log.handler_logger_email.exception(message)
                except:
                    _logger.exception("Email exception notify failed.")
                else:
                    is_success = True                    

            if mr.config.log.DO_HOOK_HTTP is True:
                # We don't believe that this will actually fail due to non-
                # resolution/etc.

                try:
                    mr.config.log.handler_logger_http.exception(message_with_exception)
                except:
                    _logger.exception("HTTP exception notify failed.")
                else:
                    is_success = True

            if is_success is False:
                raise SystemError("Could not send exception notification.")
        else:
            _logger.warning("Exception notifications aren't hooked.")
            _logger.exception(*args, **kwargs)

_notify = None
def get_notify():
    global _notify

    if _notify is None:
        _notify = _ExceptionNotifyWrapper()

    return _notify

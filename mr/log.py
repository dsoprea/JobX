import sys
import logging
import traceback
import functools

import mr.config.log

_logger = logging.getLogger(__name__)

_handler_logger = logging.getLogger('MR_HANDLER')

handler_logger_email = _handler_logger.getChild('EMAIL')
handler_logger_http = _handler_logger.getChild('HTTP')


class _Notify(object):
    def __log(self, type_, message, *args):
        message = message % args

        message_with_extra = message
        if sys.exc_type is not None:
            # The HTTPHandler won't transfer the traceback.
            message_with_extra += '\n\n' + traceback.format_exc()

        if mr.config.log.DO_HOOK_EMAIL is True or \
           mr.config.log.DO_HOOK_HTTP is True:
            is_success = False

            if mr.config.log.DO_HOOK_EMAIL is True:
                try:
                    handler = getattr(
                                mr.config.log.handler_logger_email, 
                                type_)

                    handler(message)
                except:
                    _logger.exception("Email exception notify failed.")
                else:
                    is_success = True                    

            if mr.config.log.DO_HOOK_HTTP is True:
                # We don't believe that this will actually fail due to non-
                # resolution/etc.

                try:
                    handler = getattr(
                                mr.config.log.handler_logger_http, 
                                type_)

                    handler(message_with_extra)
                except:
                    _logger.exception("HTTP exception notify failed.")
                else:
                    is_success = True

            if is_success is False:
                raise SystemError("Could not send exception notification.")
        else:
            _logger.warning("Exception notifications aren't hooked.")
            _logger.exception(*args, **kwargs)

    def __getattr__(self, name):
        return functools.partial(self.__log, name)

_notify = None
def get_notify():
    global _notify

    if _notify is None:
        _notify = _Notify()

    return _notify

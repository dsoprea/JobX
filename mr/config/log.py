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

handler_logger = logging.getLogger('MR_HANDLER')

handler_logger_raw = handler_logger.getChild('RAW')
handler_logger_email = handler_logger.getChild('EMAIL')
handler_logger_http = handler_logger.getChild('HTTP')

def _configure_email():
    hostname = os.environ.get('MR_LOG_EMAIL_HOSTNAME', 'localhost')
    from_email = os.environ.get('MR_LOG_EMAIL_FROM', 'mapreduce@local')
    to_email = os.environ['MR_LOG_EMAIL_TO'].split(',')
    subject = os.environ.get('MR_LOG_EMAIL_SUBJECT', 'MapReduce Notification')

    try:
        username = os.environ['MR_LOG_EMAIL_USERNAME']
        password = os.environ['MR_LOG_EMAIL_PASSWORD']
    except KeyError:
        credentials = None
    else:
        credentials = (username, password)

    try:
        key_filepath = os.environ['MR_LOG_EMAIL_SECURE_KEY_FILEPATH']
    except KeyError:
        secure = None
    else:
        try:
            certificate_filepath = os.environ['MR_LOG_EMAIL_SECURE_CERTIFICATE_FILEPATH']
        except KeyError:
            secure = (key_filepath,)
        else:
            secure = (key_filepath, certificate_filepath)

    try:
        port = os.environ['MR_LOG_EMAIL_HOST_PORT']
    except KeyError:
        mailhost = hostname
    else:
        mailhost = (hostname, int(port))

    sh = logging.handlers.SMTPHandler(
            mailhost, 
            from_email, 
            to_email, 
            subject, 
            credentials=credentials, 
            secure=secure)

    sh.setFormatter(_FORMATTER)
    handler_logger_email.addHandler(sh)

DO_HOOK_EMAIL = bool(int(os.environ.get('MR_LOG_EMAIL_HOOK', '0')))
if DO_HOOK_EMAIL is True:
    _configure_email()

def _configure_http():
    hostname = os.environ['MR_LOG_HTTP_HOSTNAME']
    path = os.environ['MR_LOG_HTTP_PATH']
    verb = os.environ.get('MR_LOG_HTTP_VERB', 'POST')

    hh = logging.handlers.HTTPHandler(host, path, method=verb)
    hh.setFormatter(_FORMATTER)

    handler_logger_http.addHandler(sh)

DO_HOOK_HTTP = bool(int(os.environ.get('MR_LOG_HTTP_HOOK', '0')))
if DO_HOOK_HTTP is True:
    _configure_http()

if mr.config.IS_DEBUG is True:
    fh = logging.FileHandler('/tmp/mr.flow.log')
    fh.setFormatter(_FORMATTER)

    flow_logger = logging.getLogger('mr.job_engine.flow')
    flow_logger.addHandler(fh)
    flow_logger.setLevel(logging.DEBUG)

    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

is_handler_debug = bool(int(os.environ.get('MR_HANDLER_DEBUG', '0')))
if is_handler_debug is True:
    fh = logging.FileHandler('/tmp/mr.handler.log')
    fh.setFormatter(_FORMATTER)
    handler_logger.addHandler(fh)

    handler_log_level = os.environ.get('MR_HANDLER_LOG_LEVEL', 'DEBUG')
    handler_logger.setLevel(getattr(logging, handler_log_level))

import os

RESULT_WRITER_FQ_CLASS = os.environ.get(
                            'MR_RESULT_WRITER_FQ_CLASS', 
                            'mr.result_writers.inline.InlineResultWriter')

# File result-writer options.

FILE_WRITER_OUTPUT_DIRECTORY = os.environ.get(
                                'MR_RESULT_FILE_WRITER_OUTPUT_PATH', 
                                '/tmp/mr_results')

FILE_WRITER_WRITE_AS_DIRECTORY = bool(int(os.environ.get(
                                    'MR_RESULT_FILE_WRITER_WRITE_AS_DIRECTORY',
                                    '0')))

# Email result-writer options.

EMAIL_WRITER_MIMETYPE = 'application/json'
EMAIL_WRITER_ATTACHMENT_FILENAME = 'result.json'

_DEFAULT_EMAIL_WRITER_TO_LIST = os.environ.get('MR_RESULT_EMAIL_WRITER_TO_LIST', '')
EMAIL_WRITER_TO_LIST = _DEFAULT_EMAIL_WRITER_TO_LIST.split(',') \
                        if _DEFAULT_EMAIL_WRITER_TO_LIST != '' \
                        else []

EMAIL_WRITER_FULL_FROM_NAME = 'JobX Result'

_DEFAULT_EMAIL_WRITER_SUBJECT_TEMPLATE = "Request has completed under job [%(job_name)s] (workflow [%(workflow_name)s])"
EMAIL_WRITER_SUBJECT_TEMPLATE = os.environ.get(
                                    'MR_RESULT_EMAIL_WRITER_SUBJECT_TEMPLATE', 
                                    _DEFAULT_EMAIL_WRITER_SUBJECT_TEMPLATE)

_DEFAULT_EMAIL_WRITER_TEXT_BODY_TEMPLATE = """\
Request ID: %(request_id)s

The result has been attached.
"""

EMAIL_WRITER_TEXT_BODY_TEMPLATE = os.environ.get(
                                    'MR_RESULT_EMAIL_WRITER_TEXT_BODY_TEMPLATE',
                                    _DEFAULT_EMAIL_WRITER_TEXT_BODY_TEMPLATE)

# HTTP result-writer options.

HTTP_WRITER_SERVER_URL = os.environ.get('MR_RESULT_HTTP_WRITER_SERVER_URL', '')
HTTP_WRITER_VERB = 'post'

import os

RESULT_WRITER_FQ_CLASS = os.environ.get(
                            'MR_RESULT_WRITER_FQ_CLASS', 
                            'mr.result_writers.inline.InlineResultWriter')

FILE_WRITER_OUTPUT_DIRECTORY = os.environ.get(
                                'MR_FILE_WRITER_OUTPUT_PATH', 
                                '/tmp/mr_results')

FILE_WRITER_WRITE_AS_DIRECTORY = bool(int(os.environ.get(
                                    'MR_FILE_WRITER_WRITE_AS_DIRECTORY',
                                    '0')))

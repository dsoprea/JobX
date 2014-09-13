import os

RESULT_WRITER_FQ_CLASS = os.environ.get(
                            'MR_RESULT_WRITER_FQ_CLASS', 
                            'mr.result_writers.inline.InlineResultWriter')

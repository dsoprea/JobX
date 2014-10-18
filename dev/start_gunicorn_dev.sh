#!/bin/sh

#MR_USE_FAKE_QUEUE=1 \
#MR_FAKE_QUEUE_SPOOL_PATH=$CWD/../fake_spool_path \
#MR_MULTITHREADED=0 \
#MR_RESULT_WRITER_FQ_CLASS=mr.result_writers.file.FileResultWriter \
#MR_RESULT_WRITER_FQ_CLASS=mr.result_writers.inline.InlineResultWriter \

#MR_LOG_EMAIL_HOOK=1 \
#MR_LOG_EMAIL_FROM=dustin@openpeak.com \
#MR_LOG_EMAIL_TO=dustin@openpeak.com \

PYTHONPATH=test/scope \
MR_WORKFLOW_SCOPE_FACTORY_FQ_CLASS=test_scope.WorkflowScopeFactory \
DEBUG=1 \
MR_WORKFLOW_NAMES=dev \
MR_RESULT_WRITER_FQ_CLASS=mr.result_writers.file.FileResultWriter \
MR_FS_FACTORY_FQ_CLASS=mr.fs.backend.tahoe.TahoeFilesystemFactory \
MR_FS_TAHOE_DIR_URI=URI:DIR2:kqrikhh3qfjsiphlsy7mbzdmq4:hbrnj7w64zuxgjylyxkvxtv36pasqfu2xipin7de4uboicfnbbla \
MR_FS_TAHOE_WEBAPI_URL_PREFIX=http://localhost:3456 \
MR_DO_CLEANUP_REQUESTS=1 \
MR_HANDLER_DEBUG=1 \
../mr/resources/scripts/mr_start_gunicorn_dev

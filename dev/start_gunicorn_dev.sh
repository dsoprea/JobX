#!/bin/sh

#MR_USE_FAKE_QUEUE=1 \
#MR_FAKE_QUEUE_SPOOL_PATH=$CWD/../fake_spool_path \
#MR_MULTITHREADED=0 \

PYTHONPATH=test/scope \
MR_WORKFLOW_SCOPE_FACTORY_FQ_CLASS=test_scope.WorkflowScopeFactory \
DEBUG=1 \
MR_WORKFLOW_NAMES=dev \
MR_RESULT_WRITER_FQ_CLASS=mr.result_writers.file.FileResultWriter \
MR_DO_CLEANUP_REQUESTS=0 \
MR_FS_FACTORY_FQ_CLASS=mr.fs.backend.tahoe.TahoeFilesystemFactory \
MR_FS_TAHOE_DIR_URI=URI:DIR2:um3z3xblctnajmaskpxeqvf3my:fevj3z54toroth5eeh4koh5axktuplca6gfqvht26lb2232szjoq \
MR_FS_TAHOE_WEBAPI_URL_PREFIX=http://dustinhub:3456 \
../mr/resources/scripts/mr_start_gunicorn_dev

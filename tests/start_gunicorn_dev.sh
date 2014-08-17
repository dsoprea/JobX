#!/bin/sh

cd ../mr/resources/scripts

CWD=`pwd`

DEBUG=1 
MR_WORKFLOW_NAMES=dev \
MR_USE_FAKE_QUEUE=1 \
MR_FAKE_QUEUE_SPOOL_PATH=$CWD/../fake_spool_path \
IS_MULTITHREADED=0 \
./mr_start_gunicorn_dev 

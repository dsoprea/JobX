#!/bin/sh

cd ../mr/resources/scripts

PYTHONPATH=test/scope \
MR_WORKFLOW_SCOPE_FACTORY_FQ_CLASS=test_scope.WorkflowScopeFactory \
./mr_start_gunicorn_dev

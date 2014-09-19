#!/bin/sh

DEBUG=1 ../mr/resources/scripts/mr_kv_step_create test_workflow step2 "test step: yielding and sum" handler3_mapper_yielding handler2_reducer

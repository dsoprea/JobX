#!/bin/sh

DEBUG=1 ../mr/resources/scripts/mr_kv_step_create test_workflow step1 "test step" handler1_mapper handler2_reducer

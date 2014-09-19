#!/bin/sh

DEBUG=1 ../mr/resources/scripts/mr_kv_step_create test_workflow step4 "Yields many steps" handler4_mapper_yielding_2 handler2_reducer

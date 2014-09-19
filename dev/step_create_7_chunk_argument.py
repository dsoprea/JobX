#!/bin/sh

DEBUG=1 ../mr/resources/scripts/mr_kv_step_create test_workflow step7_chunk_argument "Split the integer into many pairs" map_test_random_yield_grouped '' reduce_test_sum

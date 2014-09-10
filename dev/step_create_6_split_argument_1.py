#!/bin/sh

DEBUG=1 ../mr/resources/scripts/mr_kv_step_create dev step6_split_argument_1 "Split the argument in half" map_test_split '' reduce_test_noop

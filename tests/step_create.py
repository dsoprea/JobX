#!/bin/sh

cat resources/test_step.py | DEBUG=1 ../mr/resources/scripts/mr_kv_step_create dev step1 "test step" python -a arg1 -a arg2

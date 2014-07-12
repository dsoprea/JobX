#!/bin/sh

cat resources/test_step.py | DEBUG=1 ../mr/resources/scripts/mr_kv_step_create -a arg1 int -a arg2 int dev step1 "test step" python

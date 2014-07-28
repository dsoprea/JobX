#!/bin/sh

cat resources/step2.py | DEBUG=1 ../mr/resources/scripts/mr_kv_handler_create -a arg1 int -a arg2 int dev handler2 "test handler 2" python

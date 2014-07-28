#!/bin/sh

cat resources/step1.py | DEBUG=1 ../mr/resources/scripts/mr_kv_handler_create -a arg1 int -a arg2 int dev handler1 "test handler" python

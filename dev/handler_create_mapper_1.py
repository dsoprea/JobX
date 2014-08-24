#!/bin/sh

NAME=handler1_mapper

cat resources/$NAME.py | DEBUG=1 ../mr/resources/scripts/mr_kv_handler_create -a arg1 int -a arg2 int dev $NAME "mapper test handler 1" python mapper

#!/bin/sh

NAME=handler2_reducer

cat resources/$NAME.py | DEBUG=1 ../mr/resources/scripts/mr_kv_handler_create -a results list dev $NAME "reducer test handler 2" python reducer

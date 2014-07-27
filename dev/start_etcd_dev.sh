#!/bin/sh

etcd -addr=127.0.0.1:4001 -peer-addr=127.0.0.1:7001 -data-dir=etcd -name=machine0

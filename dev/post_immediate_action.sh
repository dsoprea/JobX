#!/bin/sh

curl -s -X POST -H "Content-Type: application/json" -d '{ "arguments": { "arg1": 11, "arg2": 22 } }' http://mapreduce.local/job/dev/job1 && echo

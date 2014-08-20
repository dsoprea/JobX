#!/bin/sh

curl -s -X POST -H "Content-Type: application/json" -d '{ "arguments": { "arg1": 144 } }' http://mapreduce.local/job/dev/job2 && echo

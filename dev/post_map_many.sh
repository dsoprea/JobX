#!/bin/sh

curl -s -X POST -H "Content-Type: application/json" -d '{ "arguments": { "arg1": 17413412 } }' http://mapreduce.local/job/dev/job3 && echo

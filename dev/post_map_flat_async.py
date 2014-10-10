#!/usr/bin/env python2.7

import requests
import json

# Submit the job and wait for a result.

headers = {
    'Content-Type': 'application/json',
}

is_blocking = False
is_blocking_phrase = 'true' \
                        if is_blocking is True \
                        else 'false'

parameters = {
    'blocking': is_blocking_phrase,
}

data = { 
    'arguments': { 
        'arg1': 17413412 
    } 
}

url = 'http://mapreduce.local/job/dev/job5'

r = requests.post(
        url, 
        data=json.dumps(data), 
        headers=headers, 
        params=parameters)

r.raise_for_status()

print("Request: [%s]" % (r.headers['x-mr-request-id'],))

if is_blocking is True:
    raw = r.json()

    result = dict(raw['result'])

    formatted = json.dumps(
                    result, 
                    sort_keys=True, 
                    indent=4, 
                    separators=(',', ': '))

    print(formatted)

#!/usr/bin/env python2.7

import requests
import json

headers = {
    'Content-Type': 'application/json',
}

data = { 
    'arguments': { 
        'arg1': 17413412 
    } 
}

url = 'http://mapreduce.local/job/dev/job3'

r = requests.post(url, data=json.dumps(data), headers=headers)
r.raise_for_status()

raw = r.json()
result = dict(raw['result'])

formatted = json.dumps(
                result, 
                sort_keys=True, 
                indent=4, 
                separators=(',', ': '))

print(formatted)

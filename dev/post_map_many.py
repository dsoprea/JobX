#!/usr/bin/env python2.7

import sys
import requests
import json

# Submit the job and wait for a result.

headers = {
    'Content-Type': 'application/json',
}

data = { 
    'arguments': { 
        'arg1': 17413412 
    } 
}

url = 'http://mapreduce.local/job/dev/job4'

r = requests.post(url, data=json.dumps(data), headers=headers)
r.raise_for_status()

# Print the result nice.

sys.stderr.write("Request ID: [%s]\n\n" % 
                 (r.headers['X-MR-REQUEST-ID'],))

raw = r.json()
result = dict(raw['result'])

formatted = json.dumps(
                result, 
                sort_keys=True, 
                indent=4, 
                separators=(',', ': '))

print(formatted)

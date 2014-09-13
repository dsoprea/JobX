#!/usr/bin/env python2.7

import sys
import requests
import json
import pprint

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
result = dict(raw['result']['pairs'])

# Note that we print using Python native printer rather than printing a JSON 
# structure (which we prefer because it's slighter lighter) because JSON will 
# result with forcibly-stringified keys.
pprint.pprint(result)

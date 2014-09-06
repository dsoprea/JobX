debug = 'true'
daemon = 'false'

bind = 'unix:/tmp/mr.gunicorn.sock'

# Until our packages become smaller (currently 161M), this is a safe value.
timeout = 120

errorlog = '-'
loglevel = 'warning'

worker_class = 'gevent'

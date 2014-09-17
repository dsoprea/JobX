import os.path

#user = 'www-data'
#group = 'www-data'

debug = 'false'
daemon = 'true'

bind = 'unix:/tmp/mr.gunicorn.sock'

timeout = 120

loglevel = 'info'

worker_class = 'gevent'

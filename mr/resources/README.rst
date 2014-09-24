**This project is still under active development, though largely finished. It is currently being tested in a production environment.**


==========
Vocabulary
==========




===============
Installing JobX
===============

------------
Dependencies
------------

- *go*:

    $ sudo apt-get install golang

- *etcd*:
  
    $ git clone git@github.com:coreos/etcd.git
    $ cd etcd
    $ ./build

    $ sudo mkdir /var/lib/etcd
    $ sudo bin/etcd -addr=127.0.0.1:4001 -peer-addr=127.0.0.1:7001 -data-dir=/var/lib/etcd -name=etcd1

- *nsq*:

    $ sudo apt-get install gpm
    $ mkdir ~/.go
    $ GOPATH=~/.go go get github.com/bitly/nsq/...
    $ sudo mkdir /var/lib/nsq
    $ cd /var/lib/nsq
    $ sudo ~/.go/bin/nsqlookupd
    $ sudo ~/.go/bin/nsqd --lookupd-tcp-address=127.0.0.1:4160

- Install Nginx.

-------------
Configuration
-------------

1. Configure Nginx:

    upstream mapreduce {
        server unix:/tmp/mr.gunicorn.sock fail_timeout=0;
    }

    server {
            listen 80;

            server_name job1.domain;
            keepalive_timeout 5;

            access_log /tmp/nginx-mr-access.log;
            error_log  /tmp/nginx-mr-error.log;

            location /s {
                root /usr/local/lib/python2.7/dist-packages/mr/resources/static;
                try_files $uri $uri/ =404;
            }

            location / {
                proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
                proxy_set_header Host $http_host;
                proxy_redirect off;

                proxy_pass http://mapreduce;
            }
    }

2. Create workflow:

MR_ETCD_HOST=job1.domain MR_WORKFLOW_NAMES=build mr_kv_workflow_create build "Jobs that assist build and deployment."

3. Load handlers:

..write and load handlers

4. Load steps:

..create step(s)

5. Load jobs:

..create job

6. Start:

MR_ETCD_HOST=job1.domain MR_ETCD_PORT=4001 MR_WORKFLOW_NAMES=build mr_start_gunicorn_dev 



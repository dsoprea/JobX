**This project is still under active development, though largely finished. It is currently being tested in a production environment. The documentation is being incrementally completed. **


==========
Vocabulary
==========




===============
Installing JobX
===============

------------
Dependencies
------------

- *go*::

    $ sudo apt-get install golang

- *etcd*::
  
    $ git clone git@github.com:coreos/etcd.git
    $ cd etcd
    $ ./build

    $ sudo mkdir /var/lib/etcd
    $ sudo bin/etcd -addr=127.0.0.1:4001 -peer-addr=127.0.0.1:7001 -data-dir=/var/lib/etcd -name=etcd1

- *nsq*::

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

1. Configure Nginx::

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

2. Create workflow::

    MR_ETCD_HOST=job1.domain MR_WORKFLOW_NAMES=build mr_kv_workflow_create build "Jobs that assist build and deployment."

3. Load handlers::

..write and load handlers

4. Load steps::

..create step(s)

5. Load jobs::

..create job

6. Start::

    MR_ETCD_HOST=job1.domain MR_NSQD_HOSTS=job1.domain:4150,job2.domain:4150 MR_WORKFLOW_NAMES=build mr_start_gunicorn_dev 


=================
Shared Filesystem
=================

There is general support for a distributed, shared filesystem between the handlers of the same workflow. The filesystem is for general use, such as prepopulating it with assets that are required for your operations, using it as a workspace for temporary files, as well as using it to deposit final artifacts, to be picked-up upon completion of the job. Any supported filesystem will share a common interface. Currently, there is only support for Tahoe LAFS.

You must define environment variables with the required parameters to enable this functionality.


========
Sessions
========

When it comes to flow, mappers receive the data (key-value pairs), first. If this data represents actual arguments, then your logic might be determining how to route execution dynamically. Your mapper may branch to downstream mappers in order to collect data that you require to perform your primary task, and your reducer may then act on it. However, the reducer may need access to some of the same data that your mapper had. Unfortunately, where the mapper receives data that it is free to slice and reorganize, the reducer only receives a collection of results from mappers that yielded data. Unless the mappers forwarded data down to the eventual result (potentially being of no actual use to any of them), the reducer may need more information to complete its task.

This is what *sessions* are for. Every mapper invocation is given a private, durable namespace in which to stash data that only the corresponding reducer will have access to. This data will be destroyed at the completion of the request like all of the other request-specific data.

There are tools available to debug sessions, if needed.


===============
Scope Factories
===============

*Scope factories* are a mechanism that allow you to inject variables into the global scope of each handler. A different scope factory can be defined for each workflow. Though you can inject the same variables into the scope of every handler [in the same workflow], the scope-factory will also receive the name of the handler. This allows you to provide sensitive information to some, but not all, handlers.

You must define environment variables with the required parameters to enable this functionality.


============
Capabilities
============

*Capabilities* are classifications that you may define to control how jobs are assigned to workers. Every worker declares a list of offered capabality classifications, and every handler declares a "required capability" classification. You may use this functionality to only route operations with handlers that invoke licensed functionality to only those workers that have been adequately equipped.


===================
Language Processors
===================

Handlers can be defined in any language, as long as there's a processor defined for it that can dispatch the code to be executed, and can translte the code that's returned to yield, and act like a generator (all handlers are generators).


===========================
Distributed Queue Semantics
===========================

The circulatory system of JobX is *bit.ly*'s NSQ platform, a very high-volume, and easy-to-deploy distributed queue.


=====================================
Distributed Storage Backend Semantics
=====================================

All persistence is done into the *etcd* distributed, immediately-consistent KV. *etcd* is a component of *CoreOS*, and is also very easy to deploy. *etcd* stores key-value pairs, but the three-things that makes it unique are:

1. It's immediately consistent across all instances.
2. Key-value pairs can be stored heirarchically.
3. You can long-poll on nodes while waiting for them to change, rather than polling them.

All data is manipulated as entities, which are modeled heirarchically on to the KV in functionality that was written specifically for this project. The models of this project resemble traditional relationship models found in Django and SQLAlchemy (to within reason, while being pragmatic and maintaining efficiency).


Entity Types
------------

- workflow: 
- job
- step
- handler
- request
- invocation


=======
Tracing
=======

When one of your handlers eventually starts chronically raising an exception, it'll be critical to be able to investigate it. These take advantage of a common *tracing* functionality. The following tools are available.


------------------------------------------
Generating a Physical Graph Using Graphviz
------------------------------------------

---------------------------------------------------
Generating an Invocation Tree from the Command-Line
---------------------------------------------------

----------------------------------------------------------
Dumping the Invocation Tree and Data from the Command-Line
----------------------------------------------------------


**This project is still under active development, though largely finished. It is currently being tested in a production environment. The documentation is being incrementally completed.**


--------
Overview
--------

*JobX* is a Python-based MapReduce solution. The JobX project is entirely written in Python, as are the queue and KV clients. However, the actual distributed queue (*NSQ*) and distributed KV (*etcd*) are written in Go.

Many of the configuration options have reasonable defaults so as to be as simple as possible to experiment with. All you need is a local instance of *NSQ* and *etcd*, which are, themselves, almost trivial to setup.


---------------
Installing JobX
---------------


Dependencies
============

- *nginx*
- *Python 2.7 (gevent is not Python 3 compatible)*
- *go*::

    $ sudo apt-get install golang

- *etcd*::
  
    $ git clone git@github.com:coreos/etcd.git
    $ cd etcd
    $ ./build

    $ sudo mkdir /var/lib/etcd
    $ sudo bin/etcd -addr=127.0.0.1:4001 -data-dir=/var/lib/etcd -name=etcd1

- *nsq*::

    $ sudo apt-get install gpm
    $ mkdir ~/.go
    $ GOPATH=~/.go go get github.com/bitly/nsq/...

    $ sudo mkdir /var/lib/nsq
    $ cd /var/lib/nsq
    $ sudo ~/.go/bin/nsqd

- Install Nginx.


Configuration
=============

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

2. Set the following two environment variables (either in your profile, or /etc/environment)::

    export MR_ETCD_HOST=127.0.0.1
    export MR_NSQD_HOSTS=127.0.0.1
    export MR_WORKFLOW_NAMES=test_workflow

2. Create workflow::

    mr_kv_workflow_create test_workflow "Jobs that assist build and deployment."

    Even though *test_workflow* is actually the default workflow name in the system, we explicitly specify it in the variables above so that it's clear how to for when you'll need to with your own workflow.

3. Load handlers::

   ..write and load handlers

4. Load steps::

   ..create step(s)

5. Load jobs::

   ..create job

6. Start::

    mr_start_gunicorn_dev 


------------------
Handler Management
------------------

In order to both alleviate the annoyance of having to maintain current copies of the sourcecode for handlers on every job worker, we store the source-code to the KV. It is syntax-checked when loaded, the metadata header is parsed, the code is compiled, and the compiled object is committed to the "library". There is a sync script that can be sure to push updated handler code, ignored unchanged handlers, and remove handlers for which no file is found and no steps refer.

The job workers will check the KV for updates approximately every ten-seconds, and merge them.


Shared Filesystem
=================

There is general support for a distributed, shared filesystem between the handlers of the same workflow. The filesystem is for general use, such as prepopulating it with assets that are required for your operations, using it as a workspace for temporary files, as well as using it to deposit final artifacts, to be picked-up upon completion of the job. Any supported filesystem will share a common interface. Currently, there is only support for Tahoe LAFS.

You must define environment variables with the required parameters to enable this functionality.


Sessions
========

When it comes to flow, mappers receive the data (key-value pairs), first. If this data represents actual arguments, then your logic might determine what comes next dynamically. Your mapper may branch to downstream mappers in order to collect data that you require to perform your primary task, and your reducer may then act on it. However, the reducer may need access to some of the same data that your mapper had. Unfortunately, where the mapper receives data that it is free to slice and reorganize, the reducer only receives a collection of results from mappers that yielded data. Unless the mappers forwarded data down to the eventual result (potentially being of no actual use intermediary mapper), the reducer may need some of that original information to complete its task.

This is what *sessions* are for. Every mapper invocation is given a private, durable namespace in which to stash data that only the corresponding reducer will have access to. This data will be destroyed at the completion of the request like all of the other request-specific entities.

There are tools available to debug sessions, if needed.


Scope Factories
===============

*Scope factories* are a mechanism that allow you to inject variables into the global scope of each handler. A different scope factory can be defined for each workflow. Though you can inject the same variables into the scope of every handler [in the same workflow], the scope-factory will also receive the name of the handler. This allows you to provide sensitive information to some, but not all, handlers.

You must define environment variables with the required parameters to enable this functionality.


Capabilities
============

*Capabilities* are classifications that you may define to control how jobs are assigned to workers. Every worker declares a list of offered capabality classifications, and every handler declares a "required capability" classification. You may use this functionality to only route operations with handlers that invoke licensed functionality to only those workers that have been adequately equipped.


Language Processors
===================

Handlers can be defined in any language, as long as there's a processor defined for it that can dispatch the code to be executed, and can yield the data that is returned (all handlers are generators).


Result Writers
==============

A *result-writer* manages how results are transmitted, and will influence what you receive in the HTTP response.

Currently, there are two:

- *inline*: Return the data within the response. **This is the default.**
- *file*: Store to a local directory.


Notifications
=============


Email Notifications
-------------------


HTTP Notifications
------------------

- The tool for listening to HTTP notifications.


Uncaught Exceptions
-------------------


Handler Examples
================


---------------------------
Distributed Queue Semantics
---------------------------

The circulatory system of JobX is *bit.ly*'s NSQ platform, a very high-volume, and easy-to-deploy, distributed queue.


------------------------
Distributed KV Semantics
------------------------

All persistence is done into the *etcd* distributed, immediately-consistent KV. *etcd* is a component of *CoreOS*, and is also very easy to deploy. *etcd* stores key-value pairs, but the three-things that makes it unique are:

1. It's immediately consistent across all instances.
2. Key-value pairs can be stored heirarchically.
3. You can long-poll on nodes while waiting for them to change, rather than polling them.

All data is manipulated as entities, which are modeled heirarchically on to the KV in functionality that was written specifically for this project. The models of this project resemble traditional RDBMS models found in Django and SQLAlchemy (to within reason, while being pragmatic and maintaining efficiency).


Entity Types
============

- *workflow*: This is the container of all of the other entities. You may have concurrent workflows operating on the same cluster that have their own jobs, steps, and handlers defined. They are *completely* isolated at the queueing and storage levels.
- *job*: This defines the noun that you post requests to, and the initial step.
- *step*: This binds a mapper to a combiner (optional), and a combiner to a reducer.
- *handler*: This defines a single body of code for a mapper, combiner, or reducer.
- *request*: This identifies one received request, and the invocation of the first step.
- *invocation*: This is the basic unit of operation. Every time a mapper or reducer is queued, it is given its own invocation record.


Scripts
=======


Directly Reading KV Entities
============================

Where we want to read the "request" entity with the given ID under the "build" workflow::

    $ etcdctl get entities/request/build/c1ef1a0d645e9a01fae9de1b7eca412fb14372c3 | python -m json.tool 
    {
        "context": {
            "requester_ip": "127.0.0.1"
        },
        "is_done": true,
        "is_blocking": true,
        "failed_invocation_id": null,
        "invocation_id": "3c7494eb9f521d39e8609733a6d3988100540abb",
        "job_name": "obfuscate_for_clients",
        "workflow_name": "build"
    }

NOTE: *etcd* allows for modifying KV nodes via REST, as well. However, this is 
not recommended. Use the scripts to mitigate any potential mistakes, and to 
invoke any validation.


-------
Tracing
-------

When one of your handlers eventually starts chronically raising an exception, it'll be critical to be able to investigate it. The following tools are available, and take advantage of a common *tracing* functionality.

Note that in order to be able to do anything, you need to disable request-cleanup. Otherwise, every request will be immediately queued and destroyed after a result is achieved.


Generating a Physical Graph Using Graphviz
==========================================


Generating an Invocation Tree from the Command-Line
===================================================


Dumping the Invocation Tree and Data from the Command-Line
==========================================================


--------
Advanced
--------


KV Queue Collections
====================


KV Tree Collections
===================

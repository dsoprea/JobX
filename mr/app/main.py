import sys
import os
dev_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), 
            '..', 
            'resources', 
            'templates'))

sys.path.insert(0, dev_path)

import logging
import flask
import atexit

import mr.config
import mr.config.log
import mr.views.job
import mr.views.index
import mr.queue
import mr.workflow_manager
import mr.models.kv.workflow

app = flask.Flask(__name__)
app.debug = mr.config.IS_DEBUG

app.register_blueprint(mr.views.index.index_bp)
app.register_blueprint(mr.views.job.job_bp)

def _init_workflows():
    wm = mr.workflow_manager.get_wm()

# TODO(dustin): Load these from configuration.
    w = mr.models.kv.workflow.get('dev')
    wm.add(w)

_init_workflows()

def _boot_queue():
    mr.queue.boot()

_boot_queue()

def _stop_queue():
    mr.queue.stop()

atexit.register(_stop_queue)

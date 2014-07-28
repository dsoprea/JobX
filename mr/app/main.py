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

def _boot_mr():
    wm = mr.workflow_manager.get_wm()

# TODO(dustin): Make this list of workflows configurable.
    w = mr.models.kv.workflow.get('dev')
    wm.add(w)

    mr.queue.boot()

_boot_mr()

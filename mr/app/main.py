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
import mr.config.queue
import mr.config.ui
import mr.views.index
import mr.views.request
import mr.views.job
import mr.views.ui.job
import mr.queue.queue_manager
import mr.workflow_manager
import mr.models.kv.workflow

_logger = logging.getLogger(__name__)

app = flask.Flask(__name__, template_folder=mr.config.TEMPLATE_PATH)
app.debug = mr.config.IS_DEBUG

app.register_blueprint(mr.views.index.index_bp)
app.register_blueprint(mr.views.job.job_bp)
app.register_blueprint(mr.views.request.request_bp)

# TODO(dustin): Put the UI views into a sub-app.

app.register_blueprint(mr.views.ui.job.ui_job_bp)

@app.context_processor
def _inject_global_context():
    return mr.config.ui.TEMPLATE_GLOBALS

def _init_queue(workflow_names):
    """Start the queue. This is the circulatory system."""

    def _boot_queue():
        mr.queue.queue_manager.boot(workflow_names)

    _boot_queue()

    def _stop_queue():
        mr.queue.queue_manager.stop()

    atexit.register(_stop_queue)

def _init_workflows():
    """Start the workflow(s). This is the skeleton.

    A workflow is the pipeline, and is constituted of the jobs, steps, and 
    handlers that will be chained to fulfill each request.
    """

    wm = mr.workflow_manager.get_wm()
    workflow_names = mr.config.queue.get_current_workflows()

    for workflow_name in workflow_names:
        w = mr.models.kv.workflow.get(workflow_name)
        wm.add(w)

    _init_queue(workflow_names)

_init_workflows()

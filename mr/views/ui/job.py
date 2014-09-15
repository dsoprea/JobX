import logging
import flask

import mr.trace

_logger = logging.getLogger(__name__)

ui_job_bp = flask.Blueprint(
                'ui_job', 
                __name__,
                url_prefix='/ui/job')

@ui_job_bp.route('/<workflow_name>/<job_name>', methods=['GET'])
def ui_job_get_specific(workflow_name, job_name):
    return flask.render_template('ui/job/submit.html')

@ui_job_bp.route('/', methods=['GET'])
def ui_job_get_browser():
    return flask.render_template('ui/job/submit_navigate.html')

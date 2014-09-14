import logging
import flask
import json

_logger = logging.getLogger(__name__)

ui_job_bp = flask.Blueprint(
                'ui_job_submit', 
                __name__,
                url_prefix='/ui/job')

@ui_job_bp.route('/<workflow_name>/<job_name>', methods=['GET'])
def ui_job_submit(workflow_name, job_name):
    return flask.render_template('ui/job/submit.html')

@ui_job_bp.route('/', methods=['GET'])
def ui_job_submit_navigate():
    return flask.render_template('ui/job/submit_navigate.html')

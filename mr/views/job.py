import logging
import flask

import mr.workflow
import mr.request

_logger = logging.getLogger(__name__)

job_bp = flask.Blueprint(
            'job', 
            __name__,
            url_prefix='/job')


@job_bp.route('/<job>', methods=['POST'])
def job_submit(job):
    w = mr.workflow.get_workflow(111)
    r = mr.request.Request(w, job, flask.request.get_json())
    w.submit_job(r)

    result = r.wait_for_result()

    return flask.jsonify(result)

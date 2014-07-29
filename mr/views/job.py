import logging
import flask
import json

import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.request
import mr.models.kv.handler
import mr.workflow_manager

import mr.job_engine
#import mr.request

_logger = logging.getLogger(__name__)

job_bp = flask.Blueprint(
            'job', 
            __name__,
            url_prefix='/job')

def _get_arguments_from_request(required_argument_keys):
    request_data = flask.request.get_json()

    if request_data is None:
        raise ValueError("No arguments given (1)")

    try:
        request_arguments = request_data['arguments']
    except KeyError:
        raise ValueError("No arguments given (2)")

    request_argument_keys = set(request_arguments.keys())

    if request_argument_keys != required_argument_keys:
        raise ValueError("One or more arguments are missing")

    return request_arguments

@job_bp.route('/<workflow_name>/<job_name>', methods=['POST'])
def job_submit(workflow_name, job_name):
    # Use the workflow-manager in order to verify that we're managing this 
    # workflow.
    wm = mr.workflow_manager.get_wm()
    mw = wm.get(workflow_name)
    workflow = mw.workflow

    job = mr.models.kv.job.get(workflow, job_name)
    step = mr.models.kv.step.get(workflow, job.initial_step_name)
    handler = mr.models.kv.handler.get(workflow, step.handler_name)

    required_argument_keys = set(handler.argument_spec.keys())

    try:
        arguments = _get_arguments_from_request(required_argument_keys)
    except ValueError as e:
        message = "%s: %s" % (str(e), json.dumps(list(required_argument_keys)))
        return (message, 406)

    context = {
        'requester_ip': flask.request.remote_addr
    }

    request = mr.models.kv.request.Request(
                job_name=job.job_name, 
                arguments=arguments, 
                context=context)

    request.save()

    rr = mr.job_engine.get_request_receiver()
    result = rr.process_request(request)

    return flask.jsonify(result)

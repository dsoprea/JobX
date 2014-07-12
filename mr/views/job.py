import logging
import flask
import json

import mr.models.kv.workflow
import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.request

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
    w = mr.models.kv.workflow.WorkflowKv()
    workflow = w.get_by_name(workflow_name)

    j = mr.models.kv.job.JobKv()
    job = j.get_by_workflow_and_name(workflow, job_name)

    s = mr.models.kv.step.StepKv()
    step = s.get_by_workflow_and_name(workflow, job.initial_step_name)

    required_argument_keys = set(step.argument_spec.keys())

    try:
        arguments = _get_arguments_from_request(required_argument_keys)
    except ValueError as e:
        message = "%s: %s" % (str(e), json.dumps(list(required_argument_keys)))
        return (message, 406)

    r = mr.models.kv.request.RequestKv()

    context = {
        'requester_ip': flask.request.remote_addr
    }

    request = r.create_request(workflow, job, arguments, context)

    jl = mr.job_engine.JobEngine(request, workflow, job, step)
    result = jl.run(arguments)

#    w = mr.workflow.get_workflow(111)
#    r = mr.request.Request(w, job, )
#    w.submit_job(r)
#
#    result = r.wait_for_result()
#
    return flask.jsonify(result)

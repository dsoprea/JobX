import logging
import flask
import json

import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.request
import mr.models.kv.handler
import mr.models.kv.invocation
import mr.models.kv.trees.invocations
import mr.workflow_manager
import mr.job_engine
import mr.shared_types

_logger = logging.getLogger(__name__)

job_bp = flask.Blueprint(
            'job', 
            __name__,
            url_prefix='/job')

def _get_arguments_from_request():
    request_data = flask.request.get_json()

    _logger.debug("Job data:\n%s", request_data)

    if request_data is None:
        raise ValueError("No arguments given (1)")

    assert issubclass(request_data['arguments'].__class__, dict) is True

    try:
        return request_data['arguments'].items()
    except KeyError:
        raise ValueError("No arguments given (2)")

@job_bp.route('/<workflow_name>/<job_name>', methods=['POST'])
def job_submit(workflow_name, job_name):

# TODO(dustin): We need to determine whether or not a terminated connection 
#               will terminate the request. If so, we'll need to figure out 
#               how to prevent it (maybe just a Gunicorn or Nginx change).

    # Use the workflow-manager in order to verify that we're managing this 
    # workflow.
    wm = mr.workflow_manager.get_wm()
    managed_workflow = wm.get(workflow_name)
    workflow = managed_workflow.workflow

    job = mr.models.kv.job.get(workflow, job_name)
    step = mr.models.kv.step.get(workflow, job.initial_step_name)
    handler = mr.models.kv.handler.get(workflow, step.map_handler_name)

    arguments = _get_arguments_from_request()

    context = {
        'requester_ip': flask.request.remote_addr
    }

    invocation = mr.models.kv.invocation.Invocation(
                    invocation_id=None,
                    workflow_name=workflow_name,
                    step_name=step.step_name,
                    direction=mr.constants.D_MAP)

    invocation.save()

    dq = mr.models.kv.queues.dataset.DatasetQueue(
            workflow, 
            invocation,
            mr.models.kv.queues.dataset.DT_ARGUMENTS)

    for (k, v) in arguments:
        data = {
            'p': (k, v),
        }

        dq.add(data)

    it = mr.models.kv.trees.invocations.InvocationsTree(
            workflow, 
            invocation)

    it.create()

    request = mr.models.kv.request.Request(
                request_id=None,
                workflow_name=workflow_name,
                job_name=job.job_name, 
                invocation_id=invocation.invocation_id,
                context=context)

    request.save()

    flask

    _logger.debug("Received request: [%s]", request)

    message_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
                            workflow=workflow,
                            invocation=invocation,
                            request=request,
                            job=job, 
                            step=step,
                            handler=handler)

    rr = mr.job_engine.get_request_receiver()
    (request_id, result_tokens) = rr.process_request(message_parameters)

    result = {
        'request_id': request_id,
        'result': result_tokens,
    }

    raw_response = flask.jsonify(result)
    response = flask.make_response(raw_response)
    response.headers['X-MR-REQUEST-ID'] = request.request_id

    return response

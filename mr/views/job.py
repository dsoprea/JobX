import logging
import flask
import json

import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.request
import mr.models.kv.handler
import mr.models.kv.invocation
import mr.workflow_manager
import mr.job_engine
import mr.models.kv.queue
import mr.shared_types

_logger = logging.getLogger(__name__)

job_bp = flask.Blueprint(
            'job', 
            __name__,
            url_prefix='/job')

def _get_arguments_from_request():
    request_data = flask.request.get_json()

    if request_data is None:
        raise ValueError("No arguments given (1)")

    try:
        return request_data['arguments']
    except KeyError:
        raise ValueError("No arguments given (2)")

@job_bp.route('/<workflow_name>/<job_name>', methods=['POST'])
def job_submit(workflow_name, job_name):
    # Use the workflow-manager in order to verify that we're managing this 
    # workflow.
    wm = mr.workflow_manager.get_wm()
    managed_workflow = wm.get(workflow_name)
    workflow = managed_workflow.workflow

    job = mr.models.kv.job.get(workflow, job_name)
    step = mr.models.kv.step.get(workflow, job.initial_step_name)
    handler = mr.models.kv.handler.get(workflow, step.map_handler_name)

    required_argument_keys = set(handler.argument_spec.keys())

    try:
        raw_arguments = _get_arguments_from_request(required_argument_keys)
        arguments = handler.cast_arguments(raw_arguments)
    except mr.models.kv.handler.ArgumentMarshalError as e:
        return (str(e), 406)

    context = {
        'requester_ip': flask.request.remote_addr
    }

    invocation = mr.models.kv.invocation.Invocation(
                    step_name=step.step_name,
                    arguments=arguments)

    invocation.save()

    request = mr.models.kv.request.Request(
                job_name=job.job_name, 
                invocation_id=invocation.invocation_id,
                context=context)

    request.save()

    message_parameters = mr.shared_types.QUEUE_MESSAGE_PARAMETERS_CLS(
                            managed_workflow=managed_workflow,
                            invocation=invocation,
                            request=request,
                            job=job, 
                            step=step,
                            handler=handler,
                            arguments=arguments)

    rr = mr.job_engine.get_request_receiver()
    result = rr.process_request(message_parameters)

    return flask.jsonify(result)

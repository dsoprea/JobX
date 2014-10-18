import logging
import flask
import json
import socket

import mr.models.kv.job
import mr.models.kv.step
import mr.models.kv.handler
import mr.workflow_manager
import mr.job_engine

_HOSTNAME = socket.gethostname()

_logger = logging.getLogger(__name__)

job_bp = flask.Blueprint('job', __name__, url_prefix='/job')

def _get_arguments_from_request():
    request_data = flask.request.get_json()

    _logger.debug("Job data:\n%s", request_data)

    if request_data is None:
        raise ValueError("No arguments given (1)")

    assert issubclass(request_data['arguments'].__class__, dict) is True

    try:
        return request_data['arguments'].iteritems()
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

    arguments = _get_arguments_from_request()
    arguments = list(arguments)

    remote_addr_header = mr.config.request.REMOTE_ADDR_HEADER

    is_blocking = (flask.request.args.get('blocking', 'true') == 'true')

    context = {
        'requester_ip': flask.request.environ[remote_addr_header]
    }

    rr = mr.job_engine.get_request_receiver()

    message_parameters = rr.package_request(
                            workflow, 
                            job, 
                            step, 
                            handler, 
                            arguments, 
                            context, 
                            is_blocking=is_blocking)

    request = message_parameters.request

    _logger.debug("Pushing request [%s] with initial step [%s] and invocation [%s] for mapper "
                  "[%s].", request, step, message_parameters.invocation, handler)

    response = {}

    try:
        rr.push_request(message_parameters)

        if is_blocking is True:
            result = rr.block_for_result(message_parameters)

            # Some result writers will only be used when blocking and others 
            # when not blocking, and others in both. The latter will never
            # have anything to return in the request-response, so it should
            # always be None. In that case, we'll need to change it to an empty
            # dictionary, since we like to return our results as dictionaries.
            if result is None:
                _logger.debug("The result writer returned None in blocking-"
                              "mode. Kindly changing to an empty dictionary: "
                              "%s", request)
                result = {}
        else:
            result = None

    except Exception as e:
        _logger.exception("Request failed.")

        code = 500
        exception_type = e.__class__.__name__
        exception_message = e.message
    else:
        code = 200
        exception_type = None
        exception_message = None

        if is_blocking is True:
            if issubclass(result.__class__, dict) is False:
                raise ValueError("Result must be returned as a dictionary to "
                                 "the blocking request-response: %s" % 
                                 (request,))

        response['result'] = result

    raw_response = flask.jsonify(response)
    response = flask.make_response(raw_response)
    response.headers['X-MR-REQUEST-ID'] = request.request_id
    response.headers['X-FULFILLED-BY'] = _HOSTNAME

    if exception_type is not None:
        response.headers['X-MR-EXCEPTION-TYPE'] = exception_type
        response.headers['X-MR-EXCEPTION-MESSAGE'] = exception_message

    return (response, code)

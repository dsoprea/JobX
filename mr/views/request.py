import logging
import flask
import mimetypes

import mr.constants
import mr.trace

_logger = logging.getLogger(__name__)

request_bp = flask.Blueprint(
                'request', 
                __name__,
                url_prefix='/request')

@request_bp.route('/<workflow_name>/<request_id>', methods=['GET'])
def request_graph_get(workflow_name, request_id):
    workflow = mr.models.kv.workflow.get(workflow_name)
    request = mr.models.kv.request.get(workflow, request_id)

    ig = mr.trace.InvocationGraph(request)

    dot = ig.draw_graph()
    (format, image_data) = ig.get_image_data(dot)

    mimetype = mimetypes.types_map['.' + format]
    return flask.Response(image_data, mimetype=mimetype)

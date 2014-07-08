import flask

job_bp = flask.Blueprint(
            'job', 
            __name__,
            url_prefix='/job')

@job_bp.route('/<classification>')
def job_submit(classification):
    return flask.jsonify({ 'job_id': 123 })

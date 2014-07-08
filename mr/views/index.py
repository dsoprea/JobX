import flask

index_bp = flask.Blueprint(
            '', 
            __name__)

@index_bp.route('/')
def index():
    return "Map me. Reduce me."

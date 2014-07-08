import sys
import os
dev_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), 
            '..', 
            'resources', 
            'templates'))

sys.path.insert(0, dev_path)

import flask

import mr.config
import mr.views.job
import mr.views.index

app = flask.Flask(__name__)
app.debug = mr.config.IS_DEBUG

app.register_blueprint(mr.views.index.index_bp)
app.register_blueprint(mr.views.job.job_bp)

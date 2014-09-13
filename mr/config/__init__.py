import os

IS_DEBUG = bool(int(os.environ.get('DEBUG', '0')))
TEMPLATE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'resources', 'templates'))

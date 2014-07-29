import os.path

SOURCE_PATH = os.path.abspath(os.path.join(
                os.path.dirname(__file__), 
                '..', 
                'resources', 
                'handlers'))

SOURCE_FILENAME_PATTERN = 'handler_*.py'
SOURCE_FILENAME_TEMPLATE = '%(name)s.py'
SOURCE_META_FILENAME_SUFFIX = '.json'

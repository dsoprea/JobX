# Get normal URL-parsing calls.

try:
    from urlparse import parse_qsl
    from urllib import urlencode
except ImportError:
    # Python 3
    from urllib.parse import parse_qsl, urlencode

# Get a normal string type.

try:
    # Python 2
    basestring = basestring
except NameError:
    # Python 3
    basestring = str

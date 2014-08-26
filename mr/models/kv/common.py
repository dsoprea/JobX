import logging

import hashlib
import random

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


class CommonKv(object):
    @classmethod
    def key_from_identity(cls, parent, identity):
        if issubclass(parent.__class__, tuple) is False:
            parent = (parent,)

        if issubclass(identity.__class__, tuple) is False:
            identity = (identity,)

        key = parent + identity

        _logger.debug("Rendered key: [%s] [%s] => [%s]", 
                      parent, identity, key)

        return key

    @classmethod
    def make_opaque(cls):
        """Generate a unique ID string for the given type of identity."""

        # Our ID scheme is to generate SHA1s. Since these are going to be
        # unique, we'll just ignore entity_class.

        return hashlib.sha1(str(random.random()).encode('ASCII')).hexdigest()

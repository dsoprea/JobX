import logging
import hashlib
import random

import mr.compat

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


    @classmethod
    def flatten_identity(cls, identity):
        """Derive a key from the identity string. It can either be a flat 
        string or a tuple of flat-strings. The latter will be collapsed to a
        path name (for heirarchical storage). We may or may not do something
        with hyphens in the future.
        """

        if issubclass(identity.__class__, tuple) is True:
            for part in identity:
                if issubclass(part.__class__, mr.compat.basestring) and \
                   ('-' in part or '/' in part):
                    raise ValueError("Identity has reserved characters: [%s]" % 
                                     (identity,))

            key = '/' + '/'.join([str(part) for part in identity])
        else:
            if issubclass(part.__class__, mr.compat.basestring) and \
               ('-' in identity or '/' in identity):
                raise ValueError("Identity has reserved characters: [%s]" % 
                                 (identity,))

            key = identity

        _logger.debug("Flattening identity: [%s] => [%s]", identity, key)

        return key

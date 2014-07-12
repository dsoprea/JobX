import datetime
import random
import hashlib


class Handlers(object):
    """The base-class of our handler code libraries."""

    def __init__(self, workflow):
        self.__state = None

        self.__update_handlers()

    def run_handler(self, name, arguments, **scope_references):
        locals_ = {}
        locals_.update(arguments)
        locals_.update(scope_references)

        exec(self.__state[2][name], globals, locals_)

    def __compile(self, code_lines):
            id_ = hashlib.sha1(random.random()).hexdigest()
            code = "def " + id_ + "(args):\n" + \
                   "\n".join(('  ' + line) for line in code_lines) + "\n"

            c = compile(code, name, 'exec')
            locals_ = {}
            exec(c, globals, locals_)

            return locals_[id_]

    def __update_handlers(self):
        """Get a list of handlers and the classifications that they represent.
        """

# TODO(dustin): Load the handlers from etcd.

        sum_function = """\
return xrange(arg1)
"""

        handlers = {}

        def add_handler(name, code_lines):
            handlers[name] = self.__compile(code_lines)

        self.__state = (1, datetime.datetime.now(), handlers)

    @property
    def list_version(self):
        return self.__state[0]

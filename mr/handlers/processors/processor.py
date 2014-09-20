class Processor(object):
    def compile(self, name, arg_names, code):
        raise NotImplementedError()

    def run(self, compiled, arguments):
        raise NotImplementedError()

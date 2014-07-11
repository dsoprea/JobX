class Request(object):
    """Originates a job for an incoming request."""

    def __init__(self, workflow, job, arguments):

        self.__job = job
        self.__arguments = arguments

    def wait_for_result(self):
        raise NotImplementedError()

    @property
    def job(self):
        return self.__job

    @property
    def arguments(self):
        return self.__arguments

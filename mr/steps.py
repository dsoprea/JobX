class Steps(object):
    """The main repository of jobs and steps."""

    def __init__(self, workflow):
# TODO(dustin): Load the steps.
        raise NotImplementedError()

    def __load_steps(self):
        raise NotImplementedError()

    def __load_jobs(self):
        raise NotImplementedError()

    def get_step_by_name(self, name):
        raise NotImplementedError()

    def add_step(self, name, description, handler, argument_names=[]):
        raise NotImplementedError()

    def add_job(self, name, description, start_step, argument_names=[]):
        raise NotImplementedError()

    def get_first_step_for_job(self, job_name):
        raise NotImplementedError()

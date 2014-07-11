class _Job(object):
    def __init__(self, workflow, name, description, start_step, 
                 argument_names=[]):
        raise NotImplementedError()

def get_job_from_name(name):
    raise NotImplementedError()

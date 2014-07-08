class MappingsBase(object):
    """Manages the mappings of which jobs/steps are mapped to (and reduced 
    from) which jobs/steps.
    """

    def set_job_to_step_mapping(self, job, step):
        raise NotImplementedError()

    def set_step_to_step_mapping(self, step_from, step_to):
        raise NotImplementedError()

    def set_step_to_step_reduction(self, step_from, step_to):
        raise NotImplementedError()

    def set_step_to_job_reduction(self, step, job):
        raise NotImplementedError()

    def get_mapping_from_job(self, job_name):
        raise NotImplementedError()

    def get_mapping_from_step(self, step_name):
        raise NotImplementedError()

    def get_reduction_from_step(self, step_name):
        raise NotImplementedError()

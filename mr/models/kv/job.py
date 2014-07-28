import mr.constants
import mr.models.kv.model
import mr.models.kv.step
import mr.models.kv.workflow


class Job(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_JOB
    key_field = 'name'

    name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    initial_step_name = mr.models.kv.model.Field()

    def __init__(self, workflow=None, *args, **kwargs):
        super(Job, self).__init__(self, *args, **kwargs)

        self.__workflow = workflow

    def get_identity(self):
        return (self.__workflow.name, self.name)

    def set_workflow(self, workflow):
        self.__workflow = workflow

    @property
    def workflow(self):
        return self.__workflow

def get(workflow, job_name):
    m = Job.get_and_build((workflow.name, job_name), job_name)
    m.set_workflow(workflow)

    return m

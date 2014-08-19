import mr.constants
import mr.models.kv.model
import mr.models.kv.step
import mr.models.kv.workflow


class Job(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_JOB
    key_field = 'job_name'

    job_name = mr.models.kv.model.Field()
    workflow_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    initial_step_name = mr.models.kv.model.Field()

    def get_identity(self):
        return (self.workflow_name, self.job_name)

def get(workflow, job_name):
    m = Job.get_and_build((workflow.workflow_name, job_name), job_name)

    return m

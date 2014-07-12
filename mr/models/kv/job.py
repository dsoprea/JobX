import mr.constants
import mr.models.kv.model
import mr.entities.kv.job
import mr.entities.kv.step


class JobKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_JOB

    def create_job(self, workflow, job_name, description, initial_step):
        assert issubclass(
                initial_step.__class__, 
                mr.entities.kv.step.STEP_CLS)

        data = {
            'workflow_name': workflow.workflow_name,
            'description': description,
            'initial_step_name': initial_step.step_name
        }

        identity = (workflow.workflow_name, job_name)
        return self.create_entity(identity, data)

    def get_by_workflow_and_name(self, workflow, job_name):
        data = self.get_by_identity((workflow.workflow_name, job_name))
        return mr.entities.kv.job.JOB_CLS(job_name=job_name, **data)

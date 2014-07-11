import mr.constants
import mr.models.kv.model
import mr.entities.kv.job
import mr.entities.kv.step


class JobKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_JOB

    def create_job(self, name, description, initial_step):
        assert issubclass(
                initial_step.__class__, 
                mr.entities.kv.step.STEP_CLS)

        data = {
            'description': description,
            'initial_step_id': initial_step.id
        }

        self.create_entity(name, data)

    def get_job(self, name):
        data = self.get_by_identity(name)
        return mr.entities.kv.job.JOB_CLS(**data)

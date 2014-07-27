import mr.constants
import mr.models.kv.model
import mr.models.kv.step
import mr.models.kv.workflow


class Job(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_JOB

    job_name = mr.models.kv.model.Field()
    workflow_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    initial_step_name = mr.models.kv.model.Field()

    @classmethod
    def __build_from_data(cls, job_name, data):
        return Job(job_name=job_name, **data)

    @classmethod
    def create(cls, workflow, job_name, description, initial_step):
        assert issubclass(
                workflow.__class__, 
                mr.models.kv.workflow.Workflow)

        assert issubclass(
                initial_step.__class__, 
                mr.models.kv.step.Step)

        data = {
            'workflow_name': workflow.workflow_name,
            'description': description,
            'initial_step_name': initial_step.step_name
        }

        identity = (workflow.workflow_name, job_name)
        cls.create_entity(identity, data)

        return cls.__build_from_data(job_name, data)

    @classmethod
    def get_by_workflow_and_name(cls, workflow, job_name):
        data = cls.get_by_identity((workflow.workflow_name, job_name))
        return cls.__build_from_data(job_name, data)

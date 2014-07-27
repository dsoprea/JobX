import mr.constants
import mr.models.kv.model
import mr.models.kv.job
import mr.models.kv.workflow


class Request(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_REQUEST

    request_id = mr.models.kv.model.Field()
    workflow_name = mr.models.kv.model.Field()
    job_name = mr.models.kv.model.Field()
    arguments = mr.models.kv.model.Field()
    context = mr.models.kv.model.Field()

    @classmethod
    def __build_from_data(cls, request_id, data):
        return Request(request_id=request_id, **data)

    @classmethod
    def create(cls, workflow, job, arguments, context=None):
        assert issubclass(
                workflow.__class__,
                mr.models.kv.workflow.Workflow)

        assert issubclass(
                job.__class__,
                mr.models.kv.job.Job)

        data = {
            'workflow_name': workflow.workflow_name,
            'job_name': job.job_name,
            'arguments': arguments,
            'context': context,
        }

        request_id = cls.make_opaque()
        identity = (workflow.workflow_name, request_id)
        cls.create_entity(identity, data)
        return cls.__build_from_data(request_id, data)

    @classmethod
    def get_by_workflow_and_id(cls, workflow, request_id):
        data = cls.get_by_identity((workflow.workflow_name, request_id))
        return cls.__build_from_data(request_id, data)

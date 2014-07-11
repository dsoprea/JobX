import mr.constants
import mr.models.kv.model
import mr.entities.kv.request
import mr.entities.kv.job
import mr.entities.kv.workflow


class RequestKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_REQUEST

    def create_request(self, workflow, job, arguments):
        assert issubclass(
                workflow.__class__,
                mr.entities.kv.workflow.WORKFLOW_CLS)

        assert issubclass(
                job.__class__,
                mr.entities.kv.job.JOB_CLS)

        data = {
            'workflow_name': workflow.name,
            'job_name': job.name,
            'arguments': arguments,
        }

        return self.create_entity(data=data)

    def get_request(self, id_):
        data = self.get_by_identity(id_)
        return mr.entities.kv.request.REQUEST_CLS(**data)

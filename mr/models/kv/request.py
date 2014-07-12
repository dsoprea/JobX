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
            'workflow_name': workflow.workflow_name,
            'job_name': job.job_name,
            'arguments': arguments,
        }

        identity = (workflow.workflow_name, self.make_opaque())
        return self.create_entity(identity, data)

    def get_by_workflow_and_id(self, workflow, request_id):
        data = self.get_by_identity((workflow.name, request_id))
        return mr.entities.kv.request.REQUEST_CLS(
                request_id=request_id, 
                **data)

import mr.constants
import mr.models.kv.model
import mr.entities.kv.request
import mr.entities.kv.job
import mr.entities.kv.workflow


class RequestKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_REQUEST

    def __build_from_data(self, request_id, data):
        return mr.entities.kv.request.REQUEST_CLS(
                request_id=request_id, 
                **data)

    def create_request(self, workflow, job, arguments, context=None):
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
            'context': context,
        }

        request_id = self.make_opaque()
        identity = (workflow.workflow_name, request_id)
        self.create_entity(identity, data)
        return self.__build_from_data(request_id, data)

    def get_by_workflow_and_id(self, workflow, request_id):
        data = self.get_by_identity((workflow.workflow_name, request_id))
        return self.__build_from_data(request_id, data)

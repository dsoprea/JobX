import mr.constants
import mr.models.kv.model
import mr.models.kv.job
import mr.models.kv.workflow


class Request(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_REQUEST
    key_field = 'request_id'

    request_id = mr.models.kv.model.Field()
    workflow_name = mr.models.kv.model.Field()
    job_name = mr.models.kv.model.Field()
    invocation_id = mr.models.kv.model.Field()
    context = mr.models.kv.model.Field(is_required=False)

    # Collects the result from a step, whether it was mapped and then reduced, 
    # or whether it just performed work and returned.
    is_done = mr.models.kv.model.Field(is_required=False, default_value=False)
    failed_invocation_id = mr.models.kv.model.Field(is_required=False)
    is_blocking = mr.models.kv.model.Field(is_required=True)

    def get_identity(self):
        return (self.workflow_name, self.request_id)

def get(workflow, request_id):
    m = Request.get_and_build((workflow.workflow_name, request_id), request_id)

    return m

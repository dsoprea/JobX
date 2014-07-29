import mr.constants
import mr.models.kv.model
import mr.models.kv.job
import mr.models.kv.workflow


class Request(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_REQUEST
    key_field = 'request_id'

    request_id = mr.models.kv.model.Field()
    job_name = mr.models.kv.model.Field()
    arguments = mr.models.kv.model.Field()
    context = mr.models.kv.model.Field(is_required=False)

    def __init__(self, workflow=None, *args, **kwargs):
        super(Request, self).__init__(self, *args, **kwargs)

        self.__workflow = workflow

    def get_identity(self):
        return (self.__workflow.workflow_name, self.request_id)

    def set_workflow(self, workflow):
        self.__workflow = workflow

    @property
    def workflow(self):
        return self.__workflow

def get(workflow, request_id):
    m = Request.get_and_build((workflow.workflow_name, request_id), request_id)
    m.set_workflow(workflow)

    return m

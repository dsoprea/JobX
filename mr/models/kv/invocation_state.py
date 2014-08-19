import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow


class InvocationMap(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_INVOCATION_MAP
    key_field = 'invocation_map_id'

    invocation_map_id = mr.models.kv.model.Field()
    invocation_id = mr.models.kv.model.Field()
    is_done = mr.models.kv.model.Field()

    def __init__(self, workflow=None, *args, **kwargs):
        super(Invocation, self).__init__(*args, **kwargs)
        self.__workflow = workflow

    def get_identity(self):
        effective_parent_invocation_id = self.__parent_invocation_id \
                                             if self.__parent_invocation_id \
                                             else 0

        return (
            self.__workflow.workflow_name, 
            effective_parent_invocation_id,
            self.invocation_id)

    def set_workflow(self, workflow):
        self.__workflow = workflow

    @property
    def workflow(self):
        return self.__workflow

def get(workflow, parent_invocation_id, invocation_id):
    m = Step.get_and_build(
            (workflow.workflow_name, 
             parent_invocation_id,
             invocation_id), 
             invocation_id)

    m.set_workflow(workflow)

    return m


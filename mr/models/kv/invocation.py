import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow


class Invocation(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_INVOCATION
    key_field = 'invocation_id'

    invocation_id = mr.models.kv.model.Field()
    parent_invocation_id = mr.models.kv.model.Field(is_required=False)
    step_name = mr.models.kv.model.Field()

    # For a mapping, this describes arguments. For a reduction or action step, 
    # this describes a list of one item: the result.
    arguments = mr.models.kv.model.Field()

    # The mapper will set this before it yields any downstream steps. Not set 
    # for other step-types.
    mapped_count = mr.models.kv.model.Field(is_required=False)

    # This will be assigned at the same time as mapped_count, and decremented 
    # as downstream children of an invoked step are finished.
    mapped_waiting = mr.models.kv.model.Field(is_required=False)

    # Collects the result from a step, whether it was mapped and then reduced, 
    # or whether it just performed work and returned.
    result = mr.models.kv.model.Field(is_required=False)

    # Contains scalar exception info, or None.
    error = mr.models.kv.model.Field(is_required=False)

    def __init__(self, workflow=None, *args, **kwargs):
        super(Invocation, self).__init__(self, *args, **kwargs)
        self.__workflow = workflow

    def get_identity(self):
        return (
            self.__workflow.workflow_name, 
            self.parent_invocation_id if self.parent_invocation_id else 0,
            self.invocation_id)

    def set_workflow(self, workflow):
        self.__workflow = workflow

    @property
    def workflow(self):
        return self.__workflow

def get(workflow, invocation_id):
    m = Step.get_and_build(
            (workflow.workflow_name, invocation_id), 
            invocation_id)

    m.set_workflow(workflow)

    return m


import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow
import mr.constants


class Invocation(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_INVOCATION
    key_field = 'invocation_id'

    invocation_id = mr.models.kv.model.Field()
    created_timestamp = mr.models.kv.model.TimestampField()
    workflow_name = mr.models.kv.model.Field()
    parent_invocation_id = mr.models.kv.model.Field(is_required=False)
    step_name = mr.models.kv.model.Field()

    # The mapper will set this before it yields any downstream steps. Not set 
    # for other step-types.
    mapped_count = mr.models.kv.model.Field(is_required=False)

    # This will be assigned at the same time as mapped_count, and decremented 
    # as downstream children of an invoked step are finished.
    mapped_waiting = mr.models.kv.model.Field(is_required=False)

    # Contains scalar exception traceback.
    error = mr.models.kv.model.Field(is_required=False)

    direction = mr.models.kv.model.EnumField(mr.constants.DIRECTIONS)

    def get_identity(self):
        return (self.workflow_name, self.invocation_id)

def get(workflow, invocation_id):
    m = Invocation.get_and_build(
            (workflow.workflow_name, 
             invocation_id), 
            invocation_id)

    return m

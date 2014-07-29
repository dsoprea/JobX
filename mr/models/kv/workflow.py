import logging

import mr.constants
import mr.models.kv.model

_logger = logging.getLogger(__name__)


class Workflow(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_WORKFLOW
    key_field = 'workflow_name'

    workflow_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    handlers_state = mr.models.kv.model.Field(is_required=False)

    def get_identity(self):
        return self.workflow_name

    def __hash__(self):
        return hash(self.workflow_name)

    def __eq__(self, o):
        return o and self.workflow_name == o.workflow_name

    def __ne__(self, o):
        return o is None or self.workflow_name != o.workflow_name

def get(workflow_name):
    return Workflow.get_and_build(workflow_name, workflow_name)

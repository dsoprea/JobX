import logging

import mr.constants
import mr.models.kv.model

_logger = logging.getLogger(__name__)


class Workflow(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_WORKFLOW
    key_field = 'name'

    name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    handlers_state = mr.models.kv.model.Field()

    def get_identity(self):
        return self.name

def get(workflow_name):
    return Workflow.get_and_build(workflow_name, workflow_name)

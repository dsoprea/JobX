import logging

import mr.constants
import mr.models.kv.model

_logger = logging.getLogger(__name__)


class Workflow(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_WORKFLOW

    workflow_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    handlers_state = mr.models.kv.model.Field()

    @classmethod
    def create(cls, workflow_name, description):
        data = {
            'description': description,
            'handlers_state': '',
        }

        return cls.create_entity(workflow_name, data)

#    def update_workflow(self, workflow):
## TODO(dustin): Doesn't support assignment.
#        data = {
#            'description': workflow.description,
#            'handler_state': workflow.handler_state,
#        }
#
#        self.update_entity(workflow.workflow_name, data)

    @classmethod
    def delete_by_name(cls, workflow_name):
        cls.delete_entity(workflow_name)

    @classmethod
    def get_by_name(cls, workflow_name):
        data = cls.get_by_identity(workflow_name)
        return Workflow(workflow_name=workflow_name, **data)

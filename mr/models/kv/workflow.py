import logging

import mr.constants
import mr.models.kv.model
import mr.entities.kv.workflow

_logger = logging.getLogger(__name__)


class WorkflowKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_WORKFLOW

    def create_workflow(self, workflow_name, description):
        data = {
            'description': description,
        }

        self.create_entity(workflow_name, data)

    def get_by_name(self, workflow_name):
        data = self.get_by_identity(workflow_name)
        return mr.entities.kv.workflow.WORKFLOW_CLS(
                workflow_name=workflow_name, 
                **data)

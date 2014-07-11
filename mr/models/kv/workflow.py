import logging

import mr.constants
import mr.models.kv.model
import mr.entities.kv.workflow

_logger = logging.getLogger(__name__)


class WorkflowKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_WORKFLOW

    def create_workflow(self, name, description):
        data = {
            'description': description,
        }

        self.create_entity(name, data)

    def get_workflow_by_name(self, name):
        data = self.get_by_identity(name)
        return mr.entities.kv.workflow.WORKFLOW_CLS(**data)


class InvokedWorkflowKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_INVOKED_WORKFLOW

    def create_invoked_workflow(self, workflow, job):
        assert issubclass(
                workflow.__class__,
                mr.entities.kv.workflow.WORKFLOW_CLS)

        assert issubclass(
                job.__class__,
                mr.entities.kv.job.JOB_CLS)

        data = {
            'workflow_name': workflow.name,
            'job_name': job.name,
        }

        return self.create_identity(data=data)

    def get_invoked_workflow(self, id_):
        data = self.client.get_encoded(id_)
        return mr.entities.kv.workflow.INVOKED_WORKFLOW_CLS(**data)

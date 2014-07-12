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


class InvokedWorkflowKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_INVOKED_WORKFLOW

    def create_invoked_workflow(self, workflow, job, context=None):
        assert issubclass(
                workflow.__class__,
                mr.entities.kv.workflow.WORKFLOW_CLS)

        assert issubclass(
                job.__class__,
                mr.entities.kv.job.JOB_CLS)

        data = {
            'workflow_name': workflow.workflow_name,
            'job_name': job.job_name,
            'context': context,
        }

        id_ = self.make_opaque()
        identity = (workflow.workflow_name, id_)
        self.create_entity(identity, data)
        return id_

    def get_by_workflow_and_id(self, workflow, invoked_workflow_id):
        identity = (workflow.workflow_name, invoked_workflow_id)
        data = self.get_by_identity(identity)
        return mr.entities.kv.workflow.INVOKED_WORKFLOW_CLS(
                invoked_workflow_id=invoked_workflow_id, 
                **data)

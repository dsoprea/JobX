import hashlib
import sys

import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow


class Handler(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_HANDLER

    workflow_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    source = mr.models.kv.model.Field()
    version = mr.models.kv.model.Field()

    @classmethod
    def create(cls, workflow, handler_name, description, source, 
               version):
        assert issubclass(
                workflow.__class__,
                mr.models.kv.workflow.Workflow)

        data = {
            'workflow_name': workflow.workflow_name,
            'description': description,
            'source': source,
            'version': version }

        return self.create_entity((workflow.workflow_name, handler_name), data)

    @classmethod
    def get_by_workflow_and_name(cls, workflow, handler_name):
        data = self.get_by_identity((workflow.workflow_name, handler_name))
        return Handler(handler_name=handler_name, **data)

    def update(self, **kwargs):
        super(Handler, self).update(**kwargs)

        return self.__class__.update_entity(
                (workflow.workflow_name, handler_name), 
                self.get_data())

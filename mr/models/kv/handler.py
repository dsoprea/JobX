import hashlib
import sys
import hashlib

import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow


class Handler(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_HANDLER
    key_field = 'name'

    name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    argument_spec = mr.models.kv.model.Field()
    source_type = mr.models.kv.model.Field()
    source_code = mr.models.kv.model.Field()
    version = mr.models.kv.model.Field(is_required=False)

    def __init__(self, workflow=None, *args, **kwargs):
        super(Handler, self).__init__(self, *args, **kwargs)
        self.update_version()

        self.__workflow = workflow

    def get_identity(self):
        return (self.__workflow.name, self.name)

    def update_version(self):
        self.version = hashlib.sha1(self.source_code).hexdigest()

    def presave(self):
        self.update_version()

    def set_workflow(self, workflow):
        self.__workflow = workflow

    @property
    def workflow(self):
        return self.__workflow

def get(workflow, handler_name):
    m = Handler.get_and_build(
            (workflow.name, handler_name),
            handler_name)

    m.set_workflow(workflow)

    return m

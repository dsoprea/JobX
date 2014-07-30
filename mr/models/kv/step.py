import hashlib
import sys

import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow

# Step-types

ST_MAP    = 'map'
ST_REDUCE = 'reduce'
ST_ACTION = 'action'

STEP_TYPES = (ST_MAP, ST_REDUCE, ST_ACTION)


class Step(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_STEP
    key_field = 'step_name'

    step_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    step_type = mr.models.kv.model.EnumField(['map', 'reduce', 'action'])
    handler_name = mr.models.kv.model.Field()

    def __init__(self, workflow=None, *args, **kwargs):
        super(Step, self).__init__(self, *args, **kwargs)

        self.__workflow = workflow
        self.__library = None

    def get_identity(self):
        return (self.__workflow.workflow_name, self.step_name)

    def set_workflow(self, workflow):
        self.__workflow = workflow

    @property
    def workflow(self):
        return self.__workflow

def get(workflow, step_name):
    m = Step.get_and_build((workflow.workflow_name, step_name), step_name)
    m.set_workflow(workflow)

    return m

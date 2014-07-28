import hashlib
import sys

import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow


class _StepLibrary(object):
    def __init__(self, step_kv, workflow):
        self.__s = s
        self.__workflow = workflow
        self.__steps = None

    def __str__(self):
        return "<STEP_LIBRARY (%s) steps>" % (len(self.__steps),)

    def __load(self):
# TODO(dustin): We might need a greenlet to long-poll on the steps heirarchy.
# TODO(dustin): We should put a single version/timestamp in a specific place
#               so that we can long-poll on only one key, and we can compare 
#               it's value to that of our current information.
        self.__steps = dict(self.__get_all_by_workflow())

    def __get_all_by_workflow(self):
        children_gen = self.get_children_identity(
                        self.__workflow.workflow_name)
        for node_name, data in children_gen:
            s = mr.models.kv.step.Step(step_name=node_name, **data)
            yield (node_name, s)

    def get_step(self, step_name):
        return self.__steps[step_name]


class Step(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_STEP
    key_field = 'name'

    name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()
    handler_name = mr.models.kv.model.Field()

    def __init__(self, workflow=None, *args, **kwargs):
        super(Step, self).__init__(self, *args, **kwargs)

        self.__workflow = workflow
        self.__library = None

    def get_identity(self):
        return (self.__workflow.name, self.name)

## TODO(dustin): Determine whether this should be an object method or class 
##               method.
#    def get_library_for_workflow(self, workflow):
#        if self.__library is None:
#            self.__library = _StepLibrary(self, workflow)
#
#        return self.__library
#
    def set_workflow(self, workflow):
        self.__workflow = workflow

    @property
    def workflow(self):
        return self.__workflow

def get(workflow, step_name):
    m = Step.get_and_build((workflow.name, step_name), step_name)
    m.set_workflow(workflow)

    return m

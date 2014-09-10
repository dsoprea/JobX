import collections

import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow


class Step(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_STEP
    key_field = 'step_name'

    step_name = mr.models.kv.model.Field()
    workflow_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()

    map_handler_name = mr.models.kv.model.Field()
    combine_handler_name = mr.models.kv.model.Field(is_required=False)
    reduce_handler_name = mr.models.kv.model.Field()

    def presave(self):

        # We leave it as an exercise to whomever modifies us, to test that
        # the handlers are valid. It's a very bad idea to couple classes in the 
        # same layer (especially in Python).

        assert self.map_handler_name != \
                self.combine_handler_name != \
                 self.reduce_handler_name

    def get_identity(self):
        return (self.workflow_name, self.step_name)

def get(workflow, step_name):
    m = Step.get_and_build((workflow.workflow_name, step_name), step_name)

    return m

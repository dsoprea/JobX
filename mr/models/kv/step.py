import collections

import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow

# Step-types

ST_MAP    = 'map'
ST_REDUCE = 'reduce'

STEP_TYPES = (ST_MAP, ST_REDUCE)

MAPREDUCE_HANDLERS_CLS = collections.namedtuple(
                            'MapReducerHandlers',
                            ['mapper_handler_name', 
                             'reducer_handler_name'])


class Step(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_STEP
    key_field = 'step_name'

    step_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()

    # A map handler may or may not yield additional steps. If it does, it'll 
    # fail if the reduce-handler isn't set. If no yields were done, post the
    # result immediately (ordinarily, additional steps will be yielded, and 
    # then coalesced later by a reduction step... in this case, the reducer
    # can be None)
    map_handler_name = mr.models.kv.model.Field()
    reduce_handler_name = mr.models.kv.model.Field(is_required=False)

    def __init__(self, workflow=None, *args, **kwargs):
        super(Step, self).__init__(self, *args, **kwargs)

        self.__workflow = workflow

    def presave(self):
# TODO(dustin): Validate handlers.
        pass

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

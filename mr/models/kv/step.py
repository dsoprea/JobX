import collections

import mr.constants
import mr.models.kv.model
import mr.models.kv.workflow

# Step-types

#ST_MAP    = 'map'
#ST_REDUCE = 'reduce'
#
#STEP_TYPES = (ST_MAP, ST_REDUCE)

# TODO(dustin): Still use/need this?
#MAPREDUCE_HANDLERS_CLS = collections.namedtuple(
#                            'MapReducerHandlers',
#                            ['mapper_handler_name', 
#                             'reducer_handler_name'])


class Step(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_STEP
    key_field = 'step_name'

    step_name = mr.models.kv.model.Field()
    workflow_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()

    # A map handler may or may not yield additional steps. If it does, it'll 
    # fail if the reduce-handler isn't set. If no yields were done, post the
    # result immediately (ordinarily, additional steps will be yielded, and 
    # then coalesced later by a reduction step... in this case, the reducer
    # can be None)
# TODO(dustin): We might consider storing a classification to be stored with 
#               the handler-definitions, and forcing a suffix of "_map" and 
#               "_reduce" (or just "_<classification>") to these handlers' 
#               names.
    map_handler_name = mr.models.kv.model.Field()
    combine_handler_name = mr.models.kv.model.Field(is_required=False)
    reduce_handler_name = mr.models.kv.model.Field(is_required=False)

    def presave(self):
# TODO(dustin): Validate that handlers exist.
        assert self.map_handler_name != self.reduce_handler_name

    def get_identity(self):
        return (self.workflow_name, self.step_name)

def get(workflow, step_name):
    m = Step.get_and_build((workflow.workflow_name, step_name), step_name)

    return m

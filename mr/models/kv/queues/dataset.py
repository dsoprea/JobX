import logging

import mr.models.kv.queues.queue

_logger = logging.getLogger(__name__)

# Datset types
DT_ARGUMENTS = 'arguments'
DT_POST_COMBINE = 'post_combine'
DT_POST_REDUCE = 'post_reduce'

_DATASET_TYPES = (DT_ARGUMENTS, DT_POST_COMBINE, DT_POST_REDUCE)


class DatasetQueue(mr.models.kv.queues.queue.Queue):
    queue_class = 'dataset'

    def __init__(self, workflow, invocation, dataset_type, *args, **kwargs):
        assert issubclass(
                invocation.__class__, 
                mr.models.kv.invocation.Invocation)

        assert dataset_type in _DATASET_TYPES

        self.__workflow = workflow
        self.__invocation = invocation
        self.__dataset_type = dataset_type

        super(DatasetQueue, self).__init__(*args, **kwargs)

    def get_root_tree_identity(self):
        """Returns a complete tuple that'll be flattened to the path that 
        contains the children.
        """

        return (self.__workflow.workflow_name, 
                self.__class__.queue_class, 
                self.__invocation.invocation_id,
                self.__dataset_type)
import logging

import mr.models.kv.queues.queue
import mr.models.kv.request

_logger = logging.getLogger(__name__)


class RequestCleanupQueue(mr.models.kv.queues.queue.Queue):
    queue_class = 'request_cleanup'

    def __init__(self, workflow, *args, **kwargs):
        self.__workflow = workflow

        super(RequestCleanupQueue, self).__init__(*args, **kwargs)

    def get_root_tree_identity(self):
        """Returns a complete tuple that'll be flattened to the path that 
        contains the children.
        """

        return (self.__class__.queue_class,
                self.__workflow.workflow_name)

    def get_entity_from_data(self, request_id):
        """Returns the model object for the given child."""

        return mr.models.kv.request.get(self.__workflow, request_id)

    def get_data_from_entity(self, request):
        """Derive the name/key from the given entity, with which to represent 
        the child.
        """

        return request.request_id

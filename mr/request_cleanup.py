import logging
import pprint

import mr.config
import mr.trace
import mr.models.kv.workflow
import mr.models.kv.queues.dataset
import mr.models.kv.trees.relationships

_logger = logging.getLogger(__name__)


class RequestCleanup(object):
    def __init__(self, request, just_simulate=False):
        self.__request = request
        self.__workflow = mr.models.kv.workflow.get(request.workflow_name)
        self.__just_simulate = just_simulate

    def __prune_invocation_data(self, invocation):
        """Remove associated datasets."""

        # Remove argument data.

        try:
            dqa = mr.models.kv.queues.dataset.DatasetQueue(
                    self.__workflow, 
                    invocation,
                    mr.models.kv.queues.dataset.DT_ARGUMENTS)

            if self.__just_simulate is True:
                list(dqa.list())
            else:
                dqa.delete()
        except KeyError:
            _logger.debug("No ARGUMENT datasets to be removed for invocation: "
                          "[%s]", invocation.invocation_id)
        else:
            _logger.debug("Removed ARGUMENT datasets for invocation: [%s]", 
                          invocation.invocation_id)

        # Remove post-combine data.

        try:
            dqc = mr.models.kv.queues.dataset.DatasetQueue(
                    self.__workflow, 
                    invocation,
                    mr.models.kv.queues.dataset.DT_POST_COMBINE)

            if self.__just_simulate is True:
                list(dqc.list())
            else:
                dqc.delete()
        except KeyError:
            _logger.debug("No POST-COMBINE datasets to be removed for invocation: "
                          "[%s]", invocation.invocation_id)
        else:
            _logger.debug("Removed POST-COMBINE datasets for invocation: [%s]", 
                          invocation.invocation_id)

        # Remove post-reduce data.

        try:
            dqr = mr.models.kv.queues.dataset.DatasetQueue(
                    self.__workflow, 
                    invocation,
                    mr.models.kv.queues.dataset.DT_POST_REDUCE)

            if self.__just_simulate is True:
                list(dqr.list())
            else:
                dqr.delete()
        except KeyError:
            _logger.debug("No POST-REDUCE datasets to be removed for invocation: "
                          "[%s]",
                          invocation.invocation_id)
        else:
            _logger.debug("Removed POST-REDUCE datasets for invocation: [%s]", 
                          invocation.invocation_id)

    def __prune_invocation_relationships(self, invocation):
        """Remove the associated relationship paper-trail."""

        try:
            mrt = mr.models.kv.trees.relationships.RelationshipsTree(
                    self.__workflow, 
                    invocation, 
                    mr.models.kv.trees.relationships.RT_MAPPED)

            if self.__just_simulate is True:
                list(mrt.list())
            else:
                mrt.delete()
        except KeyError:
            _logger.debug("No MAPPED relationships to remove for invocation: "
                          "[%s]", invocation.invocation_id)
        else:
            _logger.debug("Removed MAPPED relationships: [%s]", 
                          invocation.invocation_id)

        try:
            rrt = mr.models.kv.trees.relationships.RelationshipsTree(
                    self.__workflow, 
                    invocation, 
                    mr.models.kv.trees.relationships.RT_REDUCED)

            if self.__just_simulate is True:
                list(rrt.list())
            else:
                rrt.delete()
        except KeyError:
            _logger.debug("No REDUCED relationships to remove for invocation: "
                          "[%s]", invocation.invocation_id)
        else:
            _logger.debug("Removed REDUCED relationships: [%s]", 
                          invocation.invocation_id)

    def __prune_invocation(self, invocation):
        self.__prune_invocation_data(invocation)
        self.__prune_invocation_relationships(invocation)

        if self.__just_simulate is True:
            _logger.debug("SIMULATION: Not removing invocation: %s", 
                          invocation)
        else:
            _logger.debug("Removing invocation: %s", str(invocation))
            invocation.delete()

    def prune_request(self):
        _logger.info("Pruning request: [%s]", self.__request.request_id)

        # Compile a list of invocations. We'd prefer to start deleting at the 
        # leafs, but we are dealing with a graph, and not a tree, and the 
        # relationships are a bit too complicated.

        invocations = {}

        for (child_invocation, parent_invocation, is_loop_to_self) \
            in mr.trace.invocation_graph_gen(self.__workflow, self.__request):
                invocations[child_invocation.invocation_id] = child_invocation

                if parent_invocation is not None:
                    invocations[parent_invocation.invocation_id] = \
                        parent_invocation

        if mr.config.IS_DEBUG is True:
            _logger.debug("(%d) invocations to be pruned:\n%s", 
                          len(invocations), pprint.pformat(invocations.keys()))

        # Delete the invocations.

        for (invocation_id, invocation) in invocations.items():
            self.__prune_invocation(invocation)

        # Now, delete the request.

        if self.__just_simulate is True:
            _logger.debug("SIMULATION: Not removing request: %s", 
                          str(self.__request))
        else:
            _logger.debug("Removing request.")
            self.__request.delete()

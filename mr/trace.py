import Queue
import pprint

import mr.models.kv.request
import mr.models.kv.workflow
import mr.models.kv.trees.invocations
import mr.models.kv.queues.dataset
import mr.constants

def _get_child_info(workflow, child_invocation, parent_invocation=None):
    # Read arguments.

    dqa = mr.models.kv.queues.dataset.DatasetQueue(
            workflow, 
            child_invocation,
            mr.models.kv.queues.dataset.DT_ARGUMENTS)

    try:
        argument_data = list(dqa.list_data())
    except KeyError:
        argument_data = None

    # Read post-combine data.

    dqc = mr.models.kv.queues.dataset.DatasetQueue(
            workflow, 
            child_invocation,
            mr.models.kv.queues.dataset.DT_POST_COMBINE)

    try:
        post_combine_data = list(dqc.list_data())
    except KeyError:
        post_combine_data = None

    # Read post-reduce data.

    dqr = mr.models.kv.queues.dataset.DatasetQueue(
            workflow, 
            child_invocation,
            mr.models.kv.queues.dataset.DT_POST_REDUCE)

    try:
        post_reduce_data = list(dqr.list_data())
    except KeyError:
        post_reduce_data = None

    return (child_invocation, argument_data, post_combine_data, 
            post_reduce_data, parent_invocation)

def invocation_graph_gen(workflow, request):
    """Return a generator that presents every a (parent, child) tuple of 
    invocation relationships.
    """

    def get_invocation(invocation_id):
        return mr.models.kv.invocation.get(
                workflow, 
                invocation_id)

    root_invocation = get_invocation(request.invocation_id)

    child_info = _get_child_info(
                    workflow, 
                    root_invocation, 
                    parent_invocation=None)

    yield child_info

    q = Queue.Queue()
    q.put(root_invocation)

    while 1:
        try:
            parent_invocation = q.get_nowait()
        except Queue.Empty:
            break

        it = mr.models.kv.trees.invocations.InvocationsTree(
                workflow, 
                parent_invocation)

        entities = list(it.list_entities())

        for child_invocation in entities:
            child_info = _get_child_info(
                            workflow, 
                            child_invocation, 
                            parent_invocation)

            yield child_info

            q.put(child_invocation)

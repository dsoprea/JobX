import Queue
import pprint

import mr.models.kv.request
import mr.models.kv.workflow
import mr.models.kv.invocation
import mr.models.kv.trees.relationships
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

    yield (root_invocation, None)

    q = Queue.Queue()
    q.put(root_invocation)

#    membership = (root_invocation.invocation_id, 'mapped')
    membership = (root_invocation.invocation_id)
    visited_s = set([membership])
    reducer_to_parent_relations_s = set()

    while 1:
        try:
            from_invocation = q.get_nowait()
        except Queue.Empty:
            break

        was_found = False

        # Attempt to find map relationships from this invocation.

        try:
            it = mr.models.kv.trees.relationships.RelationshipsTree(
                    workflow, 
                    from_invocation,
                    mr.models.kv.trees.relationships.RT_MAPPED)

            entities = it.list_entities()

            for to_invocation in entities:
                yield (to_invocation, from_invocation)

#                membership = (to_invocation.invocation_id, 'mapped')
                membership = (to_invocation.invocation_id)
                if membership not in visited_s:
                    q.put(to_invocation)
                    visited_s.add(membership)
        except KeyError:
            pass
        else:
            was_found = True

        # Attempt to find reduce relationships from this invocation.

        try:
            it = mr.models.kv.trees.relationships.RelationshipsTree(
                    workflow, 
                    from_invocation,
                    mr.models.kv.trees.relationships.RT_REDUCED)

            entities = it.list_entities_and_data()

            for to_invocation, data in entities:
                reduce_invocation_id = data['ri']
                reduce_invocation = get_invocation(reduce_invocation_id)

                yield (reduce_invocation, from_invocation)

                reducer_parent_relation_member = \
                    (to_invocation.invocation_id, 
                     reduce_invocation.invocation_id)

                # Make sure that we only report one of the connections from the 
                # reducer to the original mapping (we have a list of all of the 
                # constituent datasets).
                if reducer_parent_relation_member not in \
                    reducer_to_parent_relations_s:
#                    membership = (to_invocation.invocation_id, 'reduced')
                    membership = (to_invocation.invocation_id)
                    if membership not in visited_s:
                        q.put(to_invocation)
                        visited_s.add(membership)

                    reducer_to_parent_relations_s.add(reducer_parent_relation_member)
                    yield (to_invocation, reduce_invocation)
        except KeyError:
            pass
        else:
            was_found = True

        if was_found is False:
            raise KeyError("Could not find relationships of any kind for "
                           "invocation [%s]." % 
                           (from_invocation.invocation_id,))


def invocation_graph_with_data_gen(workflow, request):
    """Return a generator that presents every a (parent, child) tuple of 
    invocation relationships, as well as the associated argument, map, and 
    reduce data.
    """

    for (child_invocation, parent_invocation) \
        in invocation_graph_gen(workflow, request):
            child_info = _get_child_info(
                            workflow, 
                            child_invocation, 
                            parent_invocation)

            yield child_info

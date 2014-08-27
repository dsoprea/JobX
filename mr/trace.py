import Queue

import mr.models.kv.request
import mr.models.kv.workflow
import mr.models.kv.trees.invocations
import mr.models.kv.trees.mapped_steps
import mr.constants

def invocation_graph_gen(workflow, request):
    """Return a generator that presents every a (parent, child) tuple of 
    invocation relationships.
    """

    def get_invocation(invocation_id):
        return mr.models.kv.invocation.get(
                workflow, 
                invocation_id)

    q = Queue.Queue()
    root_invocation = get_invocation(request.invocation_id)
    q.put(root_invocation)

    while 1:
        try:
            parent_invocation = q.get_nowait()
        except Queue.Empty:
            break

        it = mr.models.kv.trees.invocations.InvocationsTree(
                workflow, 
                parent_invocation)

        results = mr.models.kv.trees.mapped_steps.MappedStepsTree(
                    workflow, 
                    parent_invocation)

        for child_invocation in it.list_entities():
            if results is not None:
                try:
                    meta = results.get_child_meta(
                                child_invocation.invocation_id)
                except KeyError:
                    # Was not a mapping.
                    meta = None

            yield (parent_invocation, child_invocation, meta)
            q.put(child_invocation)

import Queue

import mr.models.kv.request
import mr.models.kv.workflow
import mr.models.kv.trees.invocations
import mr.models.kv.trees.mapped_steps
import mr.constants

def invocation_graph_gen(workflow, request):
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
                    result = results.get_child_meta(
                                child_invocation.invocation_id)
                except KeyError:
                    # Was not a mapping.
                    result = None

            yield (parent_invocation, child_invocation, result)
            q.put(child_invocation)

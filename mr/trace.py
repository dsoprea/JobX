import logging
import Queue
import subprocess

import graphviz

import mr.config.trace
import mr.models.kv.request
import mr.models.kv.workflow
import mr.models.kv.invocation
import mr.models.kv.trees.relationships
import mr.models.kv.queues.dataset
import mr.models.kv.job
import mr.models.kv.step
import mr.constants

_logger = logging.getLogger(__name__)


class InvocationLookupError(KeyError):
    pass

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
        try:
            return mr.models.kv.invocation.get(
                    workflow, 
                    invocation_id)
        except KeyError:
            raise InvocationLookupError(invocation_id)

    root_invocation = get_invocation(request.invocation_id)

    yield (root_invocation, None, False)

    q = Queue.Queue()
    q.put(root_invocation)

    membership = root_invocation.invocation_id
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
                yield (to_invocation, from_invocation, False)

                membership = to_invocation.invocation_id
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
                # If this is a reduction over a dataset that was yielded from a 
                # mapping (which stores data to itself as opposed to a set of 
                # datasets from downstream mappings being reduced and stored 
                # into the parent), we'll first yield the mapping->reduce 
                # relationship, and then the reduce->mapping relationship. The 
                # consumer of this generator won't know that it's a loop. So, 
                # we tell them.
                is_loop_to_self = from_invocation.invocation_id == \
                                    to_invocation.invocation_id

                reduce_invocation_id = data['ri']
                reduce_invocation = get_invocation(reduce_invocation_id)

                yield (reduce_invocation, from_invocation, is_loop_to_self)

                reducer_parent_relation_member = \
                    (to_invocation.invocation_id, 
                     reduce_invocation.invocation_id)

                # Make sure that we only report one of the connections from the 
                # reducer to the original mapping (we have a list of all of the 
                # constituent datasets).
                if reducer_parent_relation_member not in \
                   reducer_to_parent_relations_s:
                    membership = to_invocation.invocation_id
                    if membership not in visited_s:
                        q.put(to_invocation)
                        visited_s.add(membership)

                    reducer_to_parent_relations_s.add(reducer_parent_relation_member)
                    yield (to_invocation, reduce_invocation, is_loop_to_self)
        except KeyError:
            pass
        else:
            was_found = True

        # We can crash out because the whole point of this mechanism is forensics.
        if was_found is False:
            _logger.error("Could not find relationships of any kind for "
                          "invocation [%s]." % 
                          (from_invocation.invocation_id,))

def invocation_graph_with_data_gen(workflow, request):
    """Return a generator that presents every a (parent, child) tuple of 
    invocation relationships, as well as the associated argument, map, and 
    reduce data.
    """

    for (child_invocation, parent_invocation, is_loop_to_self) \
        in invocation_graph_gen(workflow, request):
            child_info = _get_child_info(
                            workflow, 
                            child_invocation, 
                            parent_invocation)

            yield (child_info, is_loop_to_self)


class InvocationGraph(object):
    def __init__(self, request):
        self.__request = request
        self.__workflow = mr.models.kv.workflow.get(request.workflow_name)

        self.__root_invocation = mr.models.kv.invocation.get(
                                    self.__workflow, 
                                    request.invocation_id)
        
        self.__job = mr.models.kv.job.get(self.__workflow, request.job_name)

    def __escape(self, text):
        return text.replace("\\", "\\\\").replace('"', '\\"')

    def __get_inv_id(self, invocation):
        return invocation.invocation_id[:10]

    def __get_inv_node_id(self, invocation):
        return 'I' + self.__get_inv_id(invocation)

    def __add_map(self, dot, invocation, parent_invocation=None):
        node_id = self.__get_inv_node_id(invocation)
        step = mr.models.kv.step.get(self.__workflow, invocation.step_name)

        label = 'S "' + self.__escape(step.step_name) + '"' + ' ' +\
                'H "' + self.__escape(step.map_handler_name) + '"' + ' ' +\
                'MI ' + self.__get_inv_id(invocation)

        dot.node(node_id, label)

        if parent_invocation is not None:
            parent_node_id = self.__get_inv_node_id(parent_invocation)
            
            if parent_invocation.direction == mr.constants.D_REDUCE:
                label = 'stored to'
            elif parent_invocation.direction == mr.constants.D_MAP:
                label = 'mapped to'
            else:
                raise ValueError("Invalid 'direction' value [%s] for "
                                 "invocation: [%s]" % 
                                 (parent_invocation.direction, 
                                  parent_invocation))

            dot.edge(parent_node_id, node_id, label=label)

    def __add_reduce(self, dot, reduce_invocation, map_invocation, 
                     is_loop_to_self):
        map_node_id = self.__get_inv_node_id(map_invocation)
        reduce_node_id = self.__get_inv_node_id(reduce_invocation)
        step = mr.models.kv.step.get(self.__workflow, reduce_invocation.step_name)

        label = 'S "' + self.__escape(step.step_name) + '"' + ' ' +\
                'H "' + self.__escape(step.reduce_handler_name) + '"' + ' ' +\
                'RI ' + self.__get_inv_id(reduce_invocation)

        dot.node(reduce_node_id, label)

        # If a mapper returns a dataset, it'll be reduced and stored back on 
        # itself.
        if is_loop_to_self is True:
            label = "data reduced by"
        
        # Else, the mapper mapped to downstream steps.
        else:
            label = "step reduced by"

        dot.edge(map_node_id, reduce_node_id, label=label)

    def __add_invocations(self, dot):
        for child_info, is_loop_to_self \
            in mr.trace.invocation_graph_with_data_gen(
                    self.__workflow, 
                    self.__request):
                (child_invocation, argument_data, post_combine_data, 
                 post_reduce_data, parent_invocation) = child_info

                if child_invocation.direction == mr.constants.D_MAP:
                    self.__add_map(
                            dot, 
                            child_invocation, 
                            parent_invocation=parent_invocation)
                elif child_invocation.direction == mr.constants.D_REDUCE:
                    self.__add_reduce(
                        dot, 
                        child_invocation, 
                        parent_invocation, 
                        is_loop_to_self)
                else:
                    raise ValueError("Invocation [%s] direction [%s] "
                                     "invalid." % 
                                     (child_invocation.invocation_id, 
                                      child_invocation.direction))

                # TODO(dustin): See if we can represent data.
                # TODO(dustin): See if we can also annotate where the data is stored for each operation (dotted-line edges?)
                # TODO(dustin): We have to represent errors.

#            if child_invocation.error is not None:
#                print("  Child invocation error:")
#                print('')
#                print('  %s' % (child_invocation.error,))
#                print('')

    def draw_graph(self):
        replacements = {
            'request_id': self.__request.request_id,
        }

        comment = mr.config.trace.GRAPH_COMMENT_TEMPLATE % replacements
        dot = graphviz.Digraph(comment=comment)

        dot.node('Q', 'Request (' + self.__request.request_id[:10] + ')')
        dot.node('W', 'Workflow (' + self.__workflow.workflow_name + ')')
        dot.node('J', 'Job (' + self.__job.job_name + ')')

# TODO(dustin): Determine how to highlight mappings in red, ascending 
#               reductions in blue, storage vectors as dotted lines, and self-
#               reductions in something else (like dotted blue).

        dot.edge('Q', 'W', label="resolve workflow")
        dot.edge('W', 'J', label="resolve job from request in workflow")

        self.__add_invocations(dot)

        root_map_inv_id = self.__get_inv_node_id(self.__root_invocation)
        dot.edge('J', root_map_inv_id)

        return dot

    def get_source(self, dot):
        return dot.source

    def get_image_data(self, dot, format=mr.config.trace.DEFAULT_IMAGE_FORMAT):
        cmd = ['dot', '-T' + format]
        p = subprocess.Popen(
                cmd, 
                stdin=subprocess.PIPE, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE)

        (stdout, stderr) = p.communicate(input=dot.source)
        r = p.wait()

        if r != 0:
            raise ValueError("DOT command failed (%d):\n"
                             "Standard output:\n%s\n"
                             "Standard error:\n%s" % 
                             (r, stdout, stderr))

        return (format, stdout)

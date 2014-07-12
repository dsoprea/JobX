import mr.imvoked_workflow


class _Workflow(object):
    """This orchestrates a job through mapping, reduction, and result."""

    def __init__(self, workflow_id):
# TODO(dustin): Load the workflow.
        raise NotImplementedError()

    def invoke(self, job):
        raise NotImplementedError()

def get_workflow(workflow_name):
    raise NotImplementedError()

def invoke_workflow(workflow, request):
    # mr.invoked_workflow.create_invoked_workflow(workflow, job):
    raise NotImplementedError()

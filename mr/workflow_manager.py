import mr.handlers.source
import mr.handlers.library
import mr.handlers.general


class _WorkflowManager(object):
    def __init__(self):
        self.__workflows = {}

    def add(self, workflow):
# TODO(dustin): Make these configurable (they implement an interface).
        s = mr.handlers.source.FilesystemSourceAdapter(workflow)
        l = mr.handlers.library.KvLibraryAdapter(workflow)

        h = mr.handlers.general.Handlers(s, l)
        self.__workflows[workflow] = h

    def get(self, workflow_name):
        return self.__workflows[workflow_name]


_wm = _WorkflowManager()

def get_wm():
    return _wm

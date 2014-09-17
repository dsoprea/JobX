import logging
import collections

import mr.config.handler
import mr.utility
import mr.handlers.source
import mr.handlers.library
import mr.handlers.general
import mr.handlers.scope
import mr.request_cleanup

_MANAGED_WORKFLOW_CLS = collections.namedtuple(
                            'ManagedWorkflow', 
                            ['workflow', 'handlers', 'cleanup_queue'])

_logger = logging.getLogger(__name__)


class _WorkflowManager(object):
    def __init__(self):
        self.__workflows = {}

        workflow_scope_fq_class = \
            mr.config.handler.WORKFLOW_SCOPE_FACTORY_FQ_CLASS

        if workflow_scope_fq_class is not None:
            _logger.info("Loading workflow-scope factory: [%s]", 
                         workflow_scope_fq_class)

            workflow_scope_factory_cls = mr.utility.load_cls_from_string(
                                            workflow_scope_fq_class)

            assert issubclass(
                    workflow_scope_factory_cls, 
                    mr.handlers.scope.WorkflowScopeFactory) is True
        
            self.__workflow_scope_factory = workflow_scope_factory_cls()
        else:
            _logger.info("No workflow-scope factory configured.")
            self.__workflow_scope_factory = None

    def add(self, workflow):
        """Register a workflow, and construct a number of global, workflow-
        specific mechanisms.
        """

        if workflow.workflow_name in self.__workflows:
            raise ValueError("Workflow already registered: [%s]" % 
                             (workflow.workflow_name,))

# TODO(dustin): Make these classes configurable (they implement an interface).
        s = mr.handlers.source.KvSourceAdapter(workflow)
        l = mr.handlers.library.KvLibraryAdapter(workflow)

        if self.__workflow_scope_factory is not None:
            hsf = self.__workflow_scope_factory.get_handler_scope_factory(
                                                    workflow)
            _logger.debug("Created handler-scope factory for workflow [%s]: "
                          "[%s]", workflow, hsf)
        else:
            hsf = None

        h = mr.handlers.general.Handlers(
                workflow, 
                s, 
                l, 
                handler_scope_factory=hsf)

        cq = mr.request_cleanup.CleanupQueue(workflow)

        self.__workflows[workflow.workflow_name] = _MANAGED_WORKFLOW_CLS(
                                                    workflow=workflow, 
                                                    handlers=h,
                                                    cleanup_queue=cq)

    def get(self, workflow_name):
        return self.__workflows[workflow_name]

_wm = _WorkflowManager()

def get_wm():
    return _wm

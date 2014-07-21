import hashlib
import sys

import mr.constants
import mr.models.kv.model
import mr.entities.kv.step
import mr.entities.kv.workflow

class _StepLibrary(object):
    def __init__(self, step_kv, workflow):
        self.__s = s
        self.__workflow = workflow
        self.__steps = None

    def __str__(self):
        return "<STEP_LIBRARY (%s) steps>" % (len(self.__steps),)

    def __load(self):
# TODO(dustin): We might need a greenlet to long-poll on the steps heirarchy.
# TODO(dustin): We should put a single version/timestamp in a specific place
#               so that we can long-poll on only one key, and we can compare 
#               it's value to that of our current information.
        self.__steps = dict(self.__get_all_by_workflow())

    def __get_all_by_workflow(self):
        children_gen = self.get_children_identity(
                        self.__workflow.workflow_name)
        for node_name, data in children_gen:
            s = mr.entities.kv.step.STEP_CLS(step_name=node_name, **data)
            yield (node_name, s)

    def get_step(self, step_name):
        return self.__steps[step_name]


class StepKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_STEP

    def __init__(self, *args, **kwargs):
        super(StepKv, self).__init__(self, *args, **kwargs)
        self.__library = None

    def create_step(self, workflow, step_name, description, argument_spec, 
                    dynamic_code):
        assert issubclass(
                workflow.__class__,
                mr.entities.kv.workflow.WORKFLOW_CLS)

        (code_type, code_body) = dynamic_code

        assert code_type in mr.constants.CODE_TYPES
        assert code_body

        for name, type_name in argument_spec.items():
            getattr(sys.modules['__builtin__'], type_name)

        code_hash = hashlib.sha1(code_body.encode('ASCII')).hexdigest()

        data = {
            'workflow_name': workflow.workflow_name,
            'description': description,
            'argument_spec': argument_spec,
            'code_hash': code_hash,
            'code_type': code_type,
            'code_body': code_body,
        }

        return self.create_entity((workflow.workflow_name, step_name), data)

    def get_by_workflow_and_name(self, workflow, step_name):
        data = self.get_by_identity((workflow.workflow_name, step_name))
        return mr.entities.kv.step.STEP_CLS(step_name=step_name, **data)

    def get_library_for_workflow(self, workflow):
        if self.__library is None:
            self.__library = _StepLibrary(self, workflow)

        return self.__library

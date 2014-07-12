import hashlib
import sys

import mr.constants
import mr.models.kv.model
import mr.entities.kv.step
import mr.entities.kv.workflow


class StepKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_STEP

    def create_step(self, workflow, step_name, description, argument_spec, 
                    dynamic_code):
        assert issubclass(
                workflow.__class__,
                mr.entities.kv.workflow.WORKFLOW_CLS)

        (code_type, code_body) = dynamic_code

        assert code_type in mr.constants.CODE_TYPES
        assert code_body

        for name, type_name in argument_spec.items():
            getattr(sys.modules['builtins'], type_name)

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

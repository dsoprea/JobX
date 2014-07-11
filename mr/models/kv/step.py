import mr.constants
import mr.models.kv.model
import mr.entities.kv.step
import mr.entities.kv.workflow


class StepKv(mr.models.kv.model.KvModel):
    entity_class = mr.constants.ID_STEP

    def create_step(self, workflow, name, description, argument_keys, 
                    dynamic_code):
        assert issubclass(
                workflow.__class__,
                mr.entities.kv.workflow.WORKFLOW_CLS)

        (code_type, code_body) = dynamic_code

        data = {
            'workflow_name': workflow.name,
            'name': name,
            'description': description,
            'argument_keys': argument_keys,
            'code_version': code_version,
            'code_type': code_type,
            'code_body': code_body,
        }

        return self.create_entity(name, data)

    def get_step(self, id_):
        data = self.get_by_identity(id_)
        return mr.entities.kv.step.STEP_CLS(**data)

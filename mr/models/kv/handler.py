import hashlib
import sys
import logging

import mr.constants
import mr.models.kv.model
import mr.handlers.general

_logger = logging.getLogger(__name__)

# Handler types.
HT_MAPPER = 'mapper'
HT_REDUCER = 'reducer'

HANDLER_TYPES = (HT_MAPPER, HT_REDUCER)

_REDUCER_ARGS_S = set(['results'])


class ArgumentMarshalError(Exception):
    pass


class Handler(mr.models.kv.model.Model):
    entity_class = mr.constants.ID_HANDLER
    key_field = 'handler_name'

    handler_name = mr.models.kv.model.Field()
    workflow_name = mr.models.kv.model.Field()
    description = mr.models.kv.model.Field()

    # This is a list of 2-tuples, so that we can maintain order.
    argument_spec = mr.models.kv.model.Field()

    source_type = mr.models.kv.model.EnumField(mr.constants.CODE_TYPES)
    source_code = mr.models.kv.model.Field()
    version = mr.models.kv.model.Field(is_required=False)
    handler_type = mr.models.kv.model.EnumField(HANDLER_TYPES)

    def get_identity(self):
        return (self.workflow_name, self.handler_name)

    def presave(self):
        if self.handler_type == HT_REDUCER:
            defined_arguments_s = set([x[0] for x in self.argument_spec])
            if defined_arguments_s != _REDUCER_ARGS_S:
                raise ValueError("Can not save reducer handler with invalid "
                                 "defined arguments: [%s] != [%s]" % 
                                 (defined_arguments_s, _REDUCER_ARGS_S))

    def postsave(self):
        mr.handlers.general.update_workflow_handle_state(self.workflow_name)

# TODO(dustin): We need to allow optional parameters, if for nothing else then 
#               backwards compatibility.
    def cast_arguments(self, arguments_dict):
        """Return the arguments cast as the appropriate types. Raise a 
        ValueError if the arguments are not fulfilled, or can not be cast.
        """

        actual_args_s = set(arguments_dict.keys())
        required_args = [name for (name, cls) in self.argument_spec]
        required_args_s = set(required_args)

        if actual_args_s != required_args_s:
            raise ValueError("Given arguments do not match required "
                             "arguments: [%s] != [%s]", 
                             actual_args_s, required_args_s)

        distilled = {}
        for name, type_name in self.argument_spec:
            datum = arguments_dict[name]
            cls = getattr(sys.modules['__builtin__'], type_name)

            try:
                typed_datum = cls(datum)
            except ValueError as e:
                raise ArgumentMarshalError("Invalid value [%s] for request "
                                           "argument [%s] of type [%s]: [%s]" %
                                           (datum, name, cls.__name__, str(e)))

            yield (name, typed_datum)

def get(workflow, handler_name):
    obj = Handler.get_and_build(
            (workflow.workflow_name, handler_name),
            handler_name)

    return obj

def create(workflow, **fields):
    obj = Handler(workflow_name=workflow.workflow_name, **fields)
    obj.save()

    return obj

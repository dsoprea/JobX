import mr.config.handler
import mr.utility

_PROCESSORS = {}

def get_processor(source_type):
    try:
        return _PROCESSORS[source_type]
    except KeyError:
        fq_processor_module = mr.config.handler.CODE_PROCESSOR_MAP[source_type]
        processor_cls = mr.utility.load_cls_from_string(fq_processor_module)

        _PROCESSORS[source_type] = processor_cls()
        return _PROCESSORS[source_type]

import logging

import mr.config.queue
import mr.utility
import mr.queue

_logger = logging.getLogger(__name__)

def _make_queue():
    _logger.info("Starting queue consumer.")

    queue_factory_cls = mr.utility.load_cls_from_string(
                            mr.config.queue.QUEUE_FACTORY_FQ_CLASS)

    factory = queue_factory_cls()

    return mr.queue.QUEUE_INSTANCE_CLS(
            consumer=factory.get_consumer(),
            producer=factory.get_producer(),
            control=factory.get_control())

_q = _make_queue()

def boot():
    _logger.info("Creating queue.")

    _q.start()

def stop():
    _logger.info("Destroying queue.")

    _q.stop()

def get_queue():
    return _q

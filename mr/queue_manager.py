import logging

import mr.config.queue
import mr.utility
import mr.queue

_logger = logging.getLogger(__name__)

def _make_queue(workflow_names):
    _logger.info("Creating queue interfaces for workflow(s): %s", 
                 workflow_names)

    queue_factory_cls = mr.utility.load_cls_from_string(
                            mr.config.queue.QUEUE_FACTORY_FQ_CLASS)

    assert issubclass(queue_factory_cls, mr.queue.QueueFactory) is True

    topics = []
    for workflow_name in workflow_names:
        replacements = {
            'workflow_name': workflow_name,
        }

        topics.append(mr.config.queue.TOPIC_NAME_MAP_TEMPLATE % replacements)
        topics.append(mr.config.queue.TOPIC_NAME_REDUCE_TEMPLATE % replacements)

    _logger.info("Generated topics from workflows:\nWorkflow(s): %s\n"
                 "Topics: %s", workflow_names, topics)

    factory = queue_factory_cls(topics)

    return mr.queue.QUEUE_INSTANCE_CLS(
            consumer=factory.get_consumer(),
            producer=factory.get_producer(),
            control=factory.get_control())

_q = None

def boot(workflow_names):
    global _q

    _q = _make_queue(workflow_names)

    _logger.info("Starting queue producer.")
    _q.control.start_producer()

    if mr.config.queue.CONSUMER_ENABLED is True:
        _logger.info("Starting queue consumer.")
        _q.control.start_consumer()
    else:
        _logger.warning("Queue consumption is disabled. Incoming requests "
                        "will be queued but not processed.")

def stop():
    _logger.info("Stopping queue producer.")
    _q.control.stop_producer()

    if mr.config.queue.CONSUMER_ENABLED is True:
        _logger.info("Stopping queue consumer.")
        _q.control.stop_consumer()
    else:
        _logger.warning("Queue")

def get_queue():
    assert _q is not None

    return _q

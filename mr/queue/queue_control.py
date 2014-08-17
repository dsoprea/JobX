class QueueControl(object):
    def start_consumer(self):
        raise NotImplementedError()

    def start_producer(self):
        raise NotImplementedError()

    def stop_consumer(self):
        raise NotImplementedError()

    def stop_producer(self):
        raise NotImplementedError()

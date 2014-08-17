class QueueFactory(object):
    def get_consumer(self):
        raise NotImplementedError()

    def get_producer(self):
        raise NotImplementedError()

    def get_control(self):
        raise NotImplementedError()

class QueueConsumer(object):
    def is_alive(self):
        """Determine if the client is still connected/healthy."""

        raise NotImplementedError()

    @property
    def topic(self):
        """What topic are we consuming?"""

        raise NotImplementedError()

    @property
    def channel(self):
        """What channel are we consuming on?"""

        raise NotImplementedError()

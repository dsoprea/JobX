"""Represents a class that knows how to present our results."""


class BaseResultWriter(object):
    def render(self, request_id, result_pair_gen):
        raise NotImplementedError()

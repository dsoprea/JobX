"""Represents a class that knows how to present our results."""


class BaseResultWriter(object):
    def render(self, request, result_pair_gen):
        raise NotImplementedError()

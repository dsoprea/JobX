"""Represents a class that knows how to present our results."""


class BaseResultWriter(object):
    def get_response_tokens(self, result_pair_gen):
        raise NotImplementedError()

"""Represents a writer that returns the result within the response."""

import mr.result_writers.base


class InlineResultWriter(mr.result_writers.base.BaseResultWriter):
     def render(self, request_id, result_pair_gen):
        return {
            'pairs': list(result_pair_gen),
        }

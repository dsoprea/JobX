"""Represents a writer that posts the results to a webserver."""

import os
import json
import requests

import mr.config.result
import mr.result_writers.base


class HttpResultWriter(mr.result_writers.base.BaseResultWriter):
    def render(self, request, result_pair_gen):

        server_url = mr.config.result.HTTP_WRITER_SERVER_URL
        assert server_url != '', \
               "Server URL is not configured."

        verb = mr.config.result.HTTP_WRITER_VERB
        encoded_data = json.dumps(list(result_pair_gen))

        headers = {
            'Content-Type': 'application/json',
            'X-REQUEST-ID': request.request_id,
            'X-WORKFLOW-NAME': request.workflow_name,
            'X-JOB-NAME': request.job_name,
        }

        r = getattr(requests, verb)(
                server_url, 
                headers=headers, 
                data=encoded_data)

        r.raise_for_status()

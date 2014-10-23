"""Represents a writer that sends the result in an email."""

import os
import json

import mr.config.result
import mr.result_writers.base
import mr.email_support


class EmailResultWriter(mr.result_writers.base.BaseResultWriter):
    def render(self, request, result_pair_gen):
        replacements = {
            'request_id': request.request_id,
            'workflow_name': request.workflow_name,
            'job_name': request.job_name,
        }

        attachment = (
            mr.config.result.EMAIL_WRITER_MIMETYPE,
            mr.config.result.EMAIL_WRITER_ATTACHMENT_FILENAME,
            json.dumps(list(result_pair_gen)))

        attachments = [attachment]

        assert mr.config.result.EMAIL_WRITER_TO_LIST, \
               "At least one TO email is not configured."

        template = mr.email_support.EmailTemplate(
                    'mr_result',
                    mr.config.result.EMAIL_WRITER_TO_LIST,
                    mr.config.result.EMAIL_WRITER_FULL_FROM_NAME,
                    mr.config.result.EMAIL_WRITER_SUBJECT_TEMPLATE,
                    mr.config.result.EMAIL_WRITER_TEXT_BODY_TEMPLATE,
                    attachments=attachments,
                    replacements=replacements)

        mime_msg = template.build_message()
        template.send_message(mime_msg)

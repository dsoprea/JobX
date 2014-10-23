import logging
import os
import email
import email.mime.base
import email.mime.multipart
import email.mime.text
import smtplib

import markdown

import mr.config.email

_logger = logging.getLogger(__name__)


class EmailTemplate(object):
    def __init__(self, name, to_list, full_from_name, subject, text_body, 
                 html_body=None, markdown_body=None, attachments=[], 
                 replacements={}):
        self.__name = name
        self.__to_list = to_list
        self.__full_from_name = full_from_name

        self.__subject = subject % replacements
        self.__text_body = text_body % replacements

        if html_body is not None:
            self.__html_body = html_body % replacements
        else:
            self.__html_body = html_body

        if markdown_body is not None:
            assert self.__html_body is None

            markdown_body = markdown_body % replacements
            self.__html_body = markdown.markdown(markdown_body)

        self.__attachments = attachments

    def build_message(self):
        _logger.debug("Building email [%s]: [%s]", self.__name, self.__subject)

        msg = email.mime.multipart.MIMEMultipart()
        msg['Subject'] = self.__subject
        msg['To'] = ', '.join(self.__to_list)
        msg['From'] = self.__full_from_name
        msg.preamble = 'You will not see this in a MIME-aware mail reader.\n'

        if self.__html_body is not None:
            html_part = email.mime.text.MIMEText(self.__html_body, 'html')
            msg.attach(html_part)

        text_part = email.mime.text.MIMEText(self.__text_body, 'plain')
        msg.attach(text_part)

        for (mimetype, filename, data) in self.__attachments:
            type_components = mimetype.split('/')
            attachment = email.mime.base.MIMEBase(*type_components)
            attachment.set_payload(data)

            email.encoders.encode_base64(attachment)

            attachment.add_header(
                'Content-Disposition', 
                'attachment', 
                filename=filename)

            msg.attach(attachment)

        return msg

    def send_message(self, mime_msg):
        _logger.debug("Sending email to [%s] using host [%s]:\n%s", 
                      self.__to_list, mr.config.email.SMTP_HOSTNAME, 
                      mime_msg)

        env_variable = 'DUI_EMAIL_' + self.__name.upper() + '_SINK_TO_FILE'
        sink_to_file = bool(int(os.environ.get(env_variable, '0'))) or \
                       bool(int(os.environ.get('DUI_EMAIL_SINK_TO_FILE', '0')))

        if sink_to_file is True:
            filepath = ('/tmp/' + self.__name.lower() + '.msg')

            _logger.warning("Sinking email to disk instead of sending: %s", 
                            filepath)

            with open(filepath, 'w') as f:
                f.write(mime_msg.as_string())
        else:
            from_email = mr.config.email.SMTP_FROM_EMAIL
            assert from_email != '', \
                   "FROM email not configured."

            s = smtplib.SMTP(mr.config.email.SMTP_HOSTNAME)
            s.sendmail(
                from_email, 
                self.__to_list, 
                mime_msg.as_string())

            s.quit()

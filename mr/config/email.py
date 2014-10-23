import os

SMTP_HOSTNAME = os.environ.get('MR_EMAIL_SMTP_HOSTNAME', 'localhost')
SMTP_FROM_EMAIL = os.environ.get('MR_EMAIL_SMTP_FROM_EMAIL', '')

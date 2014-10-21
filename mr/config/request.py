import os

CLEANUP_TIMEOUT_S = float(os.environ.get(
                        'MR_CLEANUP_MANDATORY_QUIET_PERIOD_S', 
                        '5'))

DO_CLEANUP_REQUESTS = bool(int(os.environ.get('MR_DO_CLEANUP_REQUESTS', '1')))

REMOTE_ADDR_HEADER = os.environ.get('MR_REMOTE_ADDR_HEADER', 'HTTP_X_FORWARDED_FOR')

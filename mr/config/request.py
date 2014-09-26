import os

# This dictates how many results are returned, the entity lookups that'll be 
# performed, and the number of pruning cycles that'll be performed. It should 
# be proportional to scale.
CLEANUP_BATCH_SIZE = int(os.environ.get('MR_CLEANUP_BATCH_SIZE', '1000'))

CLEANUP_MANDATORY_QUIET_PERIOD_S = float(os.environ.get(
                                    'MR_CLEANUP_MANDATORY_QUIET_PERIOD_S', 
                                    '.5'))

DO_CLEANUP_REQUESTS = bool(int(os.environ.get('MR_DO_CLEANUP_REQUESTS', '1')))

REMOTE_ADDR_HEADER = os.environ.get('MR_REMOTE_ADDR_HEADER', 'HTTP_X_FORWARDED_FOR')

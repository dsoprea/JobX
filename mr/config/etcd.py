import os

CLIENT_CONFIG = {
    'host': os.environ.get('MR_ETCD_HOST', '127.0.0.1'), 
    'port': int(os.environ.get('MR_ETCD_PORT', '4001')), 
    'is_ssl': bool(int(os.environ.get('MR_ETCD_IS_SSL', '0'))),
}

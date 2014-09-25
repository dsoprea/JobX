import os

import mr.constants
import mr.cache.redis

_CACHE_CLS_MAP = {
    mr.constants.CT_REDIS: mr.cache.redis.RedisCache,
}

_CACHE_TYPE = os.environ.get('MR_CACHE_TYPE', mr.constants.CT_REDIS)

_CACHE_CLS = _CACHE_CLS_MAP[_CACHE_TYPE]

REDIS_HOST = os.environ.get('MR_CACHE_REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('MR_CACHE_REDIS_PORT', '6379'))

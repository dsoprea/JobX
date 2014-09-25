import redis

import mr.config.cache
import mr.cache.cache


class RedisCache(mr.cache.cache.Cache):
    def __init__(self, *args, **kwargs):
        super(RedisCache, self).__init__(*args, **kwargs)
        self.__r = redis.Redis(
                    host=mr.config.cache.REDIS_HOST, 
                    port=mr.config.cache.REDIS_PORT)

    def set(self, key, value):
        self.__r.set(key, value)

    def get(self, key):
        value = self.__r.get(key)
        if value is None:
            raise KeyError("Could not find key: [%s]" % (key,))

        return value

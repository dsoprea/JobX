class Cache(object):
    def set(self, key, value):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

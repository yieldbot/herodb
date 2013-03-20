from dulwich.lru_cache import LRUCache

class LocalCache(LRUCache):

    def __contains__(self, key):
        return self.get(key, None) is not None

    def size(self):
        return len(self._cache)

    def set(self, key, value):
        self.add(key, value)

    def remove(self, key):
        self.add(key, None)

class RedisCache(object):

    def __init__(self, connection, expire=86400):
        self.connection = connection
        self.expire = expire

    def size(self):
        return self.connection.dbsize()

    def get(self, key):
        return self.connection.get(key)

    def set(self, key, value):
        with self.connection.pipeline() as pipe:
            pipe.set(key, value)
            pipe.expire(key, self.expire)
            pipe.execute()

class QueryCache(object):

    def __init__(self, backend=None, enabled=True):
        self.backend = backend
        self.enabled = enabled
        if not self.backend:
            self.backend = LocalCache(10000)
        self.reset_stats()

    def reset_stats(self):
        self.requests = 0
        self.hits = 0
        self.misses = 0

    def get_stats(self):
        return {
            'requests': self.requests,
            'hits': self.hits,
            'misses': self.misses,
            'size': self.backend.size(),
        }

    def get(self, operation, commit_sha, cb, *args):
        self.requests += 1
        if not self.enabled or not commit_sha:
            return cb(*args)
        key = (operation,) + tuple(args)
        value = None
        if commit_sha:
            value = self.backend.get(key)
        if value is not None:
            self.hits += 1
        else:
            self.misses += 1
            value = cb(*args)
            if commit_sha:
                self.backend.set(key, value)
        return value

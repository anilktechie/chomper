from __future__ import absolute_import

from chomper import config
from chomper.exceptions import NotConfigured
from chomper.readers import Reader

try:
    import redis
except ImportError:
    raise NotConfigured('Redis library not installed')


class QueueReader(Reader):
    """
    Redis queue reader

    blpop/lpop items from a Redis queue with the provided key
    """

    schemes = ['redis']

    def __init__(self, key, timeout=None, host=None, port=None, redis_args=None):
        if redis_args is None:
            redis_args = dict()

        self.key = key
        self.timeout = timeout

        host = host if host is not None else config.get('redis', 'host')
        port = port if port is not None else config.getint('redis', 'port')
        self.redis = redis.StrictRedis(host=host, port=port, **redis_args)

    def read(self):
        while True:
            data = self._pop()
            if data is None:
                break
            else:
                yield data

    def _pop(self):
        if self.timeout is not None:
            result = self.redis.blpop([self.key], self.timeout)
            if result is not None:
                source, data = result
                return data
            else:
                return None
        else:
            return self.redis.lpop(self.key)

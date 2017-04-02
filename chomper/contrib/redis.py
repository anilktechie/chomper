from __future__ import absolute_import

import six

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

    blpop/lpop items from a Redis queue with the provided key/s
    """

    schemes = ['redis']

    def __init__(self, keys, timeout=None, host=None, port=None, redis_args=None):
        if redis_args is None:
            redis_args = dict()

        if isinstance(keys, six.string_types):
            keys = [keys]

        self.keys = keys
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
            result = self.redis.blpop(self.keys, self.timeout)
            if result is not None:
                source, data = result
                return data
            else:
                return None
        else:
            for key in self.keys:
                result = self.redis.lpop(key)
                if result is not None:
                    return result
            return None

from __future__ import absolute_import

import six

from chomper import config
from chomper.exceptions import NotConfigured
from chomper.readers import Reader

try:
    import redis
except ImportError:
    raise NotConfigured('Redis library not installed')


class RedisReader(Reader):
    """
    Redis item feed

    blpop items from a Redis queue with the provided key
    """

    schemes = ['redis']
    timeout = 5

    def __init__(self, key, host=None, port=None, redis_args=None):
        if redis_args is None:
            redis_args = dict()

        if isinstance(key, six.string_types):
            key = [key]

        self.key = key

        host = host if host is not None else config.get('redis', 'host')
        port = port if port is not None else config.getint('redis', 'port')
        self.redis = redis.StrictRedis(host=host, port=port, **redis_args)

    def read(self):
        result = self.redis.blpop(self.key, self.timeout)

        if result is None:
            # There's nothing left in the queue
            yield None
        else:
            source, data = result
            yield data

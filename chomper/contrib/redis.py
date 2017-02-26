from __future__ import absolute_import

from chomper.exceptions import NotConfigured
from chomper.feeders import Feeder

try:
    import redis
except ImportError:
    raise NotConfigured('Redis library not installed')


class RedisFeeder(Feeder):
    """
    Redis item feed

    blpop items from a Redis queue with the provided key
    """

    timeout = 5

    def __init__(self, key, host='localhost', port=6379, redis_args=None):
        if redis_args is None:
            redis_args = dict()

        self.key = key
        self.redis = redis.StrictRedis(host=host, port=port, **redis_args)

    def feed(self, item):
        result = self.redis.blpop([self.key], self.timeout)

        if result is None:
            # There's nothing left in the queue
            yield None
        else:
            source, data = result
            yield data

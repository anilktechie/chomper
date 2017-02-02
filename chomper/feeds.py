import logging

try:
    import requests
except ImportError:
    requests = None

from chomper.exceptions import NotConfigured


class Feed(object):
    """
    Base class for all feeds
    """

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)


class ListFeed(Feed):
    """
    Create items from a list of dicts
    """

    def __init__(self, items):
        self.items = items

    def __call__(self):
        for item in self.items:
            yield item


class HttpFeed(Feed):
    """
    Create items from the result of an http request
    """

    def __init__(self, url, method='get', read_lines=False, skip_lines=0, **request_args):
        if requests is None:
            raise NotConfigured('HttpFeed requires the "requests" library to be installed')
        self.url = url
        self.method = method.lower()
        self.read_lines = read_lines
        self.skip_lines = skip_lines
        self.request_args = request_args

    def __call__(self):
        response = requests.request(self.method, self.url, **self.request_args)
        if self.read_lines:
            lines = response.iter_lines()
            if self.skip_lines > 0:
                for i in range(self.skip_lines):
                    next(lines)
            return lines
        else:
            return [response.content]

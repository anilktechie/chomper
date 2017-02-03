import logging

try:
    import requests
except ImportError:
    requests = None

from chomper.exceptions import NotConfigured
from chomper.loaders import *


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

    def __init__(self, items, loader=None):
        self.items = items
        self.loader = loader if loader else DictLoader()

    def __call__(self):
        for item in self.items:
            yield self.loader(item)


class HttpFeed(Feed):
    """
    Create items from the result of an http request
    """

    def __init__(self, url, method='get', loader=None, read_lines=False, skip_lines=0, **request_args):
        if requests is None:
            raise NotConfigured('HttpFeed requires the "requests" library to be installed')

        if read_lines and not loader:
            raise ValueError('Loader must be provided when reading lines from a HTTP response.')

        self.url = url
        self.method = method.lower()
        self.loader = loader
        self.read_lines = read_lines
        self.skip_lines = skip_lines
        self.request_args = request_args

    def __call__(self):
        response = requests.request(self.method, self.url, **self.request_args)

        if not self.loader or not self.read_lines:
            yield self.response_to_item(response)

        else:
            lines = response.iter_lines()

            if self.skip_lines > 0:
                for i in range(self.skip_lines):
                    next(lines)

            for line in lines:
                try:
                    yield self.loader(line)
                except ItemNotImportable as e:
                    self.logger.info(e.message)

    @staticmethod
    def response_to_item(response):
        return DictLoader()(dict(
            url=response.url,
            headers=response.headers,
            status_code=response.status_code,
            text=response.text
        ))


class CsvHttpFeed(HttpFeed):

    def __init__(self, url, columns, *args, **kwargs):
        super(CsvHttpFeed, self).__init__(url, loader=CsvLoader(columns), read_lines=True, *args, **kwargs)

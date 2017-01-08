from chomper.exceptions import NotConfigured

try:
    import requests
except ImportError:
    raise NotConfigured('Requests library not installed')

from . import Feed


class HttpFeed(Feed):

    def __init__(self, url, method='get', read_lines=False, skip_lines=0, **request_args):
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

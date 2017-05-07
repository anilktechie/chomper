try:
    import requests
except ImportError:
    requests = None

from chomper.nodes import Node, processor
from chomper.exceptions import NotConfigured, ItemNotImportable
from chomper.support import generative


class File(Node):

    def __init__(self, name):
        self._uri = None
        self._lines = False
        super(File, self).__init__(name)

    @generative
    def uri(self, uri):
        self._uri = uri

    path = uri

    @generative
    def lines(self, read_lines=True):
        self._lines = read_lines

    @processor(['empty'])
    def read(self, item):
        if self._lines:
            self.push(self._read_lines())
        else:
            self.push(self._read_file())

    def _read_lines(self):
        with open(self._uri, 'r') as f:
            for line in f:
                if line and line.strip():
                    yield line.strip()
                else:
                    # Line with only whitespace, skip it
                    continue

    def _read_file(self):
        with open(self._uri, 'r') as f:
            return f.read()


def _request_method(method):
    @generative
    def func(self, uri, **request_args):
        self._uri = uri
        self._method = method
        self._request_args = request_args
    return func


class Http(Node):

    def __init__(self, name):
        if requests is None:
            raise NotConfigured('HttpReader requires the "requests" library to be installed')

        self._uri = None
        self._lines = False
        self._method = 'get'
        self._request_args = {}

        super(Http, self).__init__(name)

    @generative
    def lines(self, read_lines=True):
        self._lines = read_lines

    get = _request_method('get')
    head = _request_method('head')
    post = _request_method('post')
    patch = _request_method('patch')
    put = _request_method('put')
    delete = _request_method('delete')
    options = _request_method('options')

    @processor(['empty'])
    def read(self, item):
        if self._lines:
            self.push(self._read_lines())
        else:
            self.push(self._read_file())

    def _read_lines(self):
        response = requests.request(self._method, self._uri, **self._request_args)

        if not response.ok:
            raise ItemNotImportable()

        if self.lines:
            for line in response.iter_lines():
                if line and line.strip():
                    yield line.strip()
                else:
                    continue

    def _read_file(self):
        response = requests.request(self._method, self._uri, **self._request_args)

        if not response.ok:
            raise ItemNotImportable()

        return response.text


class S3(Node):

    def __init__(self):
        raise NotImplementedError()


class Ftp(Node):

    def __init__(self):
        raise NotImplementedError()


class Hdfs(Node):

    def __init__(self):
        raise NotImplementedError()

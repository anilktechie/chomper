try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

try:
    import requests
except ImportError:
    requests = None

from chomper.exceptions import NotConfigured, ItemNotImportable


class Resource(object):

    def __init__(self, uri):
        self.uri = uri

        if '://' not in uri:
            u = urlparse(uri, scheme='file')
        else:
            u = urlparse(uri)

        for key in ('scheme', 'netloc', 'path', 'params', 'query', 'fragment',
                    'username', 'password', 'hostname', 'port'):
            setattr(self, key, getattr(u, key))


class Reader(object):

    schemes = None

    def read(self):
        raise NotImplementedError()

    @classmethod
    def from_uri(cls, uri, *args, **kwargs):
        return cls(Resource(uri), *args, **kwargs)

    @classmethod
    def can_read(cls, uri):
        resource = Resource(uri)
        return cls.schemes and resource.scheme in cls.schemes


class FileReader(Reader):

    schemes = ['file']

    def __init__(self, resource, lines=True):
        self.resource = resource
        self.lines = lines

    def read(self):
        with open(self.resource.uri, 'r') as f:
            if self.lines:
                for line in f:
                    if line and line.strip():
                        yield line.strip()
                    else:
                        continue
            else:
                yield f.read()


class HttpReader(Reader):

    schemes = ['http', 'https']

    def __init__(self, resource, method='get', lines=True, **request_args):
        if requests is None:
            raise NotConfigured('HttpReader requires the "requests" library to be installed')

        self.resource = resource
        self.lines = lines
        self.method = method.lower()
        self.request_args = request_args

    def read(self):
        response = requests.request(self.method, self.resource.uri, **self.request_args)

        if not response.ok:
            # TODO: How should we handle http errors?
            raise ItemNotImportable()

        if self.lines:
            for line in response.iter_lines():
                if line and line.strip():
                    yield line.strip()
                else:
                    continue
        else:
            yield response.text


class S3Reader(Reader):

    schemes = ['s3']

    def __init__(self):
        raise NotImplementedError()


class FtpReader(Reader):

    schemes = ['ftp']

    def __init__(self):
        raise NotImplementedError()


class HdfsReader(Reader):

    schemes = ['hdfs']

    def __init__(self):
        raise NotImplementedError()

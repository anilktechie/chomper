import logging
import csv
import pprint

try:
    import simplejson as json
except ImportError:
    import json

from chomper import Item
from chomper.exceptions import ItemNotImportable
from chomper.readers import FileReader, HttpReader


class Feeder(object):
    """
    Base class for all item feeders
    """

    # TODO: allow custom readers to be added
    enabled_readers = [
        FileReader,
        HttpReader
    ]

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)

    def __call__(self, item=None):
        return self.feed(item)

    def feed(self, item):
        raise NotImplementedError('Item feeders must implement feed method.')

    def get_reader(self, uri, **kwargs):
        for ReaderCls in self.enabled_readers:
            if ReaderCls.can_read(uri):
                return ReaderCls.from_uri(uri, **kwargs)
        raise ValueError('Unsupported URI protocol for "%s"' % uri)


class ListFeeder(Feeder):
    """
    Create items from a list of dicts
    """

    def __init__(self, items):
        self.items = items

    def feed(self, item):
        for item in self.items:
            yield self.parse(item)

    def parse(self, item):
        return Item(**item)


class CsvFeeder(Feeder):

    def __init__(self, uri, columns, skip=0, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL):
        self.uri = uri
        self.columns = columns
        self.skip = skip
        self.reader_args = dict(delimiter=delimiter, quotechar=quotechar, quoting=quoting)

    def feed(self, item):
        reader = self.get_reader(self.uri)
        lines = csv.reader(reader.read(), **self.reader_args)

        if self.skip > 0:
            for i in range(self.skip):
                next(lines)

        for line in lines:
            yield self.parse(line)

    def parse(self, line):
        # Make sure we have the same number of keys and values
        if len(self.columns) != len(line):
            raise ItemNotImportable('Could not import item from CSV file as the data '
                                    'did not matched the defined columns.')
        item = dict(zip(self.columns, line))
        return Item(**item)


class JsonFeeder(Feeder):

    def __init__(self, uri):
        self.uri = uri

    def feed(self, item):
        reader = self.get_reader(self.uri, lines=False)
        for data in reader.read():
            for item in self.parse(data):
                yield Item(**item)

    def parse(self, data):
        try:
            json_data = json.loads(data)
        except ValueError:
            raise ItemNotImportable('Could not load JSON string \n%r' % pprint.pformat(data))
        except TypeError:
            raise ItemNotImportable('Could not load JSON as input was not a string')

        if isinstance(json_data, list):
            return json_data
        elif isinstance(json_data, dict):
            return [json_data]
        else:
            raise ItemNotImportable('Could not load JSON as the resource did not contain list or object.')


class JsonLinesFeeder(Feeder):

    def __init__(self, uri):
        self.uri = uri

    def feed(self, item):
        reader = self.get_reader(self.uri)
        for line in reader.read():
            yield self.parse(line)

    def parse(self, line):
        try:
            return Item(json.loads(line))
        except ValueError:
            raise ItemNotImportable('Could not load JSON string \n%r' % pprint.pformat(line))
        except TypeError:
            raise ItemNotImportable('Could not load JSON as input was not a string')

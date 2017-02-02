import logging
import pprint
import csv
import six

try:
    import simplejson as json
except ImportError:
    import json

from .exceptions import ItemNotImportable
from .items import Item

class Loader(object):
    """
    Base class for all loaders
    """

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)

    @staticmethod
    def drop_empty(item):
        # This covers None and empty strings
        if not item:
            raise ItemNotImportable('Item dropped as it was empty')

        # Check if it's just a string of whitespace
        if isinstance(item, six.string_types):
            if item.strip() == '':
                raise ItemNotImportable('Item dropped as it was empty (only contained whitespace)')

        return item


class JsonLoader(Loader):

    def __call__(self, item):
        try:
            return Item(json.loads(self.drop_empty(item)))
        except ValueError:
            # Bad JSON string
            raise ItemNotImportable('Could not load JSON string \n%r' % pprint.pformat(item))
        except TypeError:
            # Not a JSON string
            raise ItemNotImportable('Could not load JSON as input was not a string')


class CsvLoader(Loader):

    def __init__(self, keys=None, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL):
        self.keys = keys
        self.reader_args = dict(delimiter=delimiter, quotechar=quotechar, quoting=quoting)

    def __call__(self, item):
        item = next(csv.reader([self.drop_empty(item)], **self.reader_args))

        # If keys are provided, convert the list to a dict
        if self.keys:
            # Make sure we have the same number of keys and values
            if len(self.keys) != len(item):
                raise ItemNotImportable()
            item = dict(zip(self.keys, item))

        return Item(item)

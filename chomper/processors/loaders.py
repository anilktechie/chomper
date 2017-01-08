import pprint
import csv

try:
    import simplejson as json
except ImportError:
    import json

from chomper.exceptions import ItemNotImportable
from . import Processor


class JsonLoader(Processor):

    def __call__(self, item):
        try:
            return json.loads(item)
        except ValueError:
            # Bad JSON string
            raise ItemNotImportable('Could not load JSON string \n%r' % pprint.pformat(item))
        except TypeError:
            # Not a JSON string
            raise ItemNotImportable('Could not load JSON as input was not a string')


class CsvLoader(Processor):

    def __init__(self, keys=None, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL):
        self.keys = keys
        self.reader_args = dict(delimiter=delimiter, quotechar=quotechar, quoting=quoting)

    def __call__(self, item):
        item = next(csv.reader([item], **self.reader_args))

        # If keys are provided, convert the list to a dict
        if self.keys:
            # Make sure we have the same number of keys and values
            if len(self.keys) != len(item):
                raise ItemNotImportable()
            item = dict(zip(self.keys, item))

        return item

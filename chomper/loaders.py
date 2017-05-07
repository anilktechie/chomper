import csv
import pprint

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

try:
    import simplejson as json
except ImportError:
    import json

from chomper.support import generative
from chomper.exceptions import ItemNotImportable
from chomper.nodes import Node, processor


class Csv(Node):

    DEFAULT_DELIMITER = ','
    DEFAULT_QUOTECHAR = '"'
    DEFAULT_QUOTING = csv.QUOTE_MINIMAL

    def __init__(self, name, columns=None):
        self._columns = columns
        self._column_types = None

        self._skip = 0

        self._delimiter = self.DEFAULT_DELIMITER
        self._quotechar = self.DEFAULT_QUOTECHAR
        self._quoting = self.DEFAULT_QUOTING

        super(Csv, self).__init__(name)

    @generative
    def columns(self, columns, types=None):
        self._columns = columns
        if types is not None:
            self._column_types = types

    @generative
    def types(self, column_types):
        self._column_types = column_types

    @generative
    def skip(self, count=0):
        self._skip = count

    @generative
    def reader(self, delimiter=DEFAULT_DELIMITER, quotechar=DEFAULT_QUOTECHAR, quoting=DEFAULT_QUOTING):
        self._delimiter = delimiter
        self._quotechar = quotechar
        self._quoting = quoting

    @processor(['iterable'])
    def process_iterable(self, item):
        self._process_iterable(item)

    @processor(['string'])
    def process_string(self, item):
        self._process_iterable(StringIO(item))

    def _process_iterable(self, iterable):
        lines = csv.reader(iterable, delimiter=self._delimiter, quotechar=self._quotechar, quoting=self._quoting)

        if self._skip > 0:
            for i in range(self._skip):
                next(lines)

        for line in lines:
            self.push(self._parse(line))

    def _parse(self, line):
        # Convert the column values to the correct type if the column data types have been defined
        if self._column_types is not None:
            line = [self._parse_column(value, self._column_types[idx]) for idx, value in enumerate(line)]

        # If columns have not been provided we can just push
        # a list of each line in the CSV file
        if self._columns is None:
            return line

        # Make sure we have the same number of keys and values
        if len(self._columns) != len(line):
            raise ItemNotImportable('Could not import item from CSV file as the data '
                                    'did not matched the defined columns.')

        columns = self._columns

        # Check if any of the column names are set to None.
        # If there are we need to remove to corresponding values from the row.
        if None in self._columns:
            line = [col for idx, col in enumerate(line) if columns is not None]
            columns = filter(None, columns)

        return dict(zip(columns, line))

    def _parse_column(self, value, value_type):
        # If the data type was None then we can convert empty values to None
        if value_type is None and not value:
            return None
        try:
            return value_type(value)
        except ValueError:
            self.logger.warning('Unable to convert value %s to type %s' % (value, value_type.__name__))
            return value


class Json(Node):

    @processor(['iterable'])
    def process_iterable(self, item):
        for data in item:
            self.push(self._parse(data))

    @processor(['string'])
    def process_string(self, item):
        self.push(self._parse(item))

    @staticmethod
    def _parse(data):
        try:
            return json.loads(data)
        except ValueError:
            raise ItemNotImportable('Could not load JSON string \n%r' % pprint.pformat(data))
        except TypeError:
            raise ItemNotImportable('Could not load JSON as input was not a string')

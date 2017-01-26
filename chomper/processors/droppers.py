import six
from chomper.exceptions import ItemNotImportable
from . import Processor


class ItemDropper(Processor):

    def __init__(self, expression):
        self.expression = expression

    def __call__(self, item):
        if item.eval(self.expression):
            raise ItemNotImportable('Item dropped as the provided expression "%s" evaluated to true.' % self.expression)
        return item


class EmptyDropper(Processor):

    def __call__(self, item):
        # This covers None, empty strings, empty lists and empty dicts
        if not item:
            raise ItemNotImportable('Item dropped as it was empty')

        # Check if it's just a string of whitespace
        if isinstance(item, six.string_types):
            if item.strip() == '':
                raise ItemNotImportable('Item dropped as it was empty (only contained whitespace)')

        return item


class ValueDropper(Processor):

    def __init__(self, key, value=None, values=None, drop_matches=True):
        self.key = key
        self.matches = drop_matches

        if values:
            self.values = values
        elif value:
            self.values = [value]
        else:
            self.values = []

    def __call__(self, item):
        if self.key not in item:
            return item

        value = item[self.key]

        if value in self.values:
            if self.matches:
                raise ItemNotImportable('Item dropped as the value of "%s" was "%s"' % (self.key, value))
        else:
            if not self.matches:
                raise ItemNotImportable('Item dropped as "%s" did not match a value in the required set.' % self.key)

        return item

import inspect
from . import Processor


class ValueFilter(Processor):

    def __init__(self, key, filter_func):
        self.key = key
        self.filter = filter_func

        if not callable(self.filter):
            raise ValueError('Filter must be callable')

    def __call__(self, item):
        try:
            value = item[self.key]
            spec = inspect.getargspec(self.filter)
            if len(spec.args) == 2:
                item[self.key] = self.filter(value, item)
            else:
                item[self.key] = self.filter(value)
        except KeyError:
            pass
        return item

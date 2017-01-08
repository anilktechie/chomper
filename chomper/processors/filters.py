from chomper.utils import smart_invoke
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
            item[self.key] = smart_invoke(self.filter, [value, item])
        except KeyError:
            pass
        except TypeError:
            pass

        return item

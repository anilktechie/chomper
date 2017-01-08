from . import Processor


class ValueMapper(Processor):

    def __init__(self, key, mapping=None):
        self.key = key
        self.mapping = mapping

        if not self.mapping or not isinstance(mapping, dict):
            raise ValueError('Mapping argument must be a dict (key is current value, value is the new value)')

    def __call__(self, item):
        try:
            value = item[self.key]
            item[self.key] = self.mapping[value]
        except KeyError:
            pass
        return item

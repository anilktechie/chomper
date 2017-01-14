import six

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


class KeyMapper(Processor):

    def __init__(self, mapping=None, **kwargs):
        if mapping is None:
            self.mapping = kwargs
        else:
            self.mapping = mapping

        if not self.mapping or not isinstance(self.mapping, dict):
            raise ValueError('Mapping argument must be a dict (key is current key name, value is the new key name)')

    def __call__(self, item):
        for current_key, new_key in six.iteritems(self.mapping):
            try:
                item[new_key] = item[current_key]
                del item[current_key]
            except KeyError:
                continue
        return item

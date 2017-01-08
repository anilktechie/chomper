import six
from . import Processor


class DefaultSetter(Processor):

    def __init__(self, *args, **kwargs):
        defaults = dict()

        if len(args) == 2 and isinstance(args[0], six.string_types):
            defaults[args[0]] = args[1]
        elif len(args) == 1 and isinstance(args[0], dict):
            defaults = args[0]
        elif len(kwargs):
            defaults = kwargs

        self.defaults = defaults

    def __call__(self, item):
        for key, default in six.iteritems(self.defaults):
            if key not in item or item[key] is None:
                item[key] = default
        return item

import six
from chomper.utils import smart_invoke
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


class FieldSetter(Processor):

    def __init__(self, key, func, cache=False):
        self.key = key
        self.func = func
        self.cache = cache
        self.result = None
        self.executed = False

    def __call__(self, item, meta, importer):
        if self.key in item:
            self.logger.debug('Field setter will override an existing field for key "%s"' % self.key)

        if self.cache and self.executed:
            item[self.key] = self.result
            return item
        else:
            self.executed = True
            self.result = self.invoke_func(item, meta, importer)
            item[self.key] = self.result
            return item

    def invoke_func(self, item, meta, importer):
        args = [item, meta, importer]
        if callable(self.func):
            return smart_invoke(self.func, args)
        elif isinstance(self.func, six.string_types) and hasattr(importer, self.func):
            return smart_invoke(getattr(importer, self.func), args)
        else:
            return self.func

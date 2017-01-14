import six
from . import Processor


class KeyRemover(Processor):

    def __init__(self, *args):
        if len(args) > 1:
            self.keys = args
        elif isinstance(args[0], list) or isinstance(args[0], tuple):
            self.keys = args
        elif isinstance(args[0], six.string_types):
            self.keys = [args[0]]
        else:
            self.keys = []
            self.logger.warn('Name of keys to be dropped must be provided')

    def __call__(self, item):
        for key in self.keys:
            try:
                del item[key]
            except KeyError:
                continue
        return item

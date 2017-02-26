import logging


class Exporter(object):
    """
    Base class for all exporters
    """

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)

    def __call__(self, item=None):
        return self.export(item)

    def export(self, item):
        raise NotImplementedError('All exporters must implement the "export" method')

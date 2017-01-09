import logging


class Exporter(object):
    """
    Base class for all exporters
    """

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)

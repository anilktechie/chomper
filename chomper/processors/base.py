import logging


class Processor(object):
    """
    Base class for all item processors
    """

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)

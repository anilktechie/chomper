import logging


class Feed(object):
    """
    Base class for all feeds
    """

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)

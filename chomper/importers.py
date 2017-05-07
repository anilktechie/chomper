import logging


class Importer(object):
    """
    An Importer is simply a container for a Pipeline.

    Custom importers should inherit from this class and override the `name` and
    `pipeline` attributes. The pipeline can also be created by adding a `.pipeline()`
    method if you prefer.
    """

    name = None
    pipeline = None

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                self.logger.warning('Importer argument "%s" overrides an existing attribute' % key)
            setattr(self, key, value)

    @property
    def logger(self):
        return logging.getLogger(self.name)

    def start(self):
        self._pipeline().start()

    def plot(self, file_name=None, kind='png'):
        if file_name is None:
            file_name = self.name
        self._pipeline().plot(file_name, kind)

    def _pipeline(self):
        try:
            return self.pipeline()
        except TypeError:
            return self.pipeline

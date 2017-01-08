import six
import time
import logging
from chomper.exceptions import ItemNotImportable


class Importer(object):
    """
    Base class for all importers
    """

    name = None
    feed = None
    processors = None
    close_when_idle = True

    def __init__(self, name=None, **kwargs):
        if name is not None:
            self.name = name

        if not self.name:
            self.name = type(self).__name__

        if self.feed is None:
            raise ValueError('Cannot create an importer without an item feed')

        if self.processors is None:
            self.processors = []

        self.items_processed = 0
        self.items_dropped = 0

        for key, value in six.iteritems(kwargs):
            setattr(self, key, value)

    @property
    def logger(self):
        return logging.getLogger(self.name)

    def process_item(self, item):
        for processor in self.processors:
            item = processor(item)
        return item

    def run(self):
        for item in self.feed.read():
            if item is None:
                break

            try:
                self.process_item(item)
            except ItemNotImportable as e:
                self.logger.info(e.message)
                self.items_dropped += 1
                continue
            else:
                self.items_processed += 1

        if not self.close_when_idle:
            time.sleep(1)
            self.run()
        else:
            self.logger.info('Importer finished, %d items were imported' % self.items_processed)

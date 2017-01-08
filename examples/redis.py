import logging
from chomper import Importer
from chomper.feeds import RedisFeed
from chomper.processors import JsonLoader, ItemLogger


logging.basicConfig(level=logging.DEBUG)


class SimpleRedisImporter(Importer):

    close_when_idle = False

    feed = RedisFeed('example_key')

    processors = [
        JsonLoader(),
        ItemLogger()
    ]


if __name__ == "__main__":
    importer = SimpleRedisImporter()
    importer.run()

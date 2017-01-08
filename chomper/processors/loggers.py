import logging
import json
from . import Processor


class ItemLogger(Processor):

    def __init__(self, level=logging.DEBUG):
        self.level = level

    def __call__(self, item, meta):
        if isinstance(item, dict):
            self.logger.log(self.level, json.dumps(item, indent=4, sort_keys=True))
        else:
            self.logger.log(self.level, str(item))
        return item


class MetaLogger(Processor):

    def __init__(self, level=logging.DEBUG):
        self.level = level

    def __call__(self, item, meta):
        self.logger.log(self.level, json.dumps(meta.__dict__, indent=4, sort_keys=True))
        return item

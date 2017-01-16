from __future__ import absolute_import

from . import Feed


class ListFeed(Feed):

    def __init__(self, items):
        self.items = items

    def __call__(self):
        for item in self.items:
            yield item

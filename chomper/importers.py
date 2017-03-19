import types
import six
import time
import logging
from copy import copy

from chomper.exceptions import ImporterMethodNotFound, ItemNotImportable, DropItem
from chomper.items import Item
from chomper.utils import smart_invoke


class ImporterMethod(object):

    def __init__(self, name):
        self.name = name


class ImporterMetaclass(type):

    def __getattr__(self, name):
        return ImporterMethod(name)


@six.add_metaclass(ImporterMetaclass)
class Importer(object):
    """
    Base class for all importers
    """

    name = None
    pipeline = None
    close_when_idle = True

    def __init__(self, name=None, **kwargs):
        if name is not None:
            self.name = name

        if not self.name:
            self.name = type(self).__name__

        if self.pipeline is None:
            self.pipeline = []

        self.items_processed = 0
        self.items_dropped = 0

        for key, value in six.iteritems(kwargs):
            setattr(self, key, value)

    @property
    def logger(self):
        return logging.getLogger(self.name)

    def run(self):
        self._traverse(Item(), copy(self.pipeline))
        self.close()

    def close(self):
        if not self.close_when_idle:
            time.sleep(1)
            self.run()
        else:
            self._close_actions(self.pipeline)

    def _close_actions(self, actions):
        for action in actions:
            if isinstance(action, list):
                self._close_actions(action)
            else:
                try:
                    action.close()
                except AttributeError:
                    continue

    def _traverse(self, item, actions):
        try:
            action = actions.pop(0)
        except IndexError:
            self.items_processed += 1
        else:
            for item in self._make_iterable(item):
                if isinstance(action, list):
                    self._traverse(copy(item), copy(actions))
                else:
                    result = self._invoke_action(action, [item, self])
                    if result:
                        self._traverse(result, copy(actions))

    @staticmethod
    def _make_iterable(item):
        is_generator = isinstance(item, types.GeneratorType)
        is_list = isinstance(item, list) or isinstance(item, tuple)
        return [item for item in (item if is_list or is_generator else [item]) if item is not None]

    def _invoke_action(self, action, action_args):
        if callable(action):
            try:
                return smart_invoke(action, action_args)
            except DropItem:
                self.items_dropped += 1
            except ItemNotImportable as e:
                self.logger.error(e.message)
                self.items_dropped += 1
        else:
            self.logger.warn('Action "%s" could not be called. Must be a callable or importer method.' % action)

    def get_method(self, method):
        if hasattr(self, method.name):
            return getattr(self, method.name)
        elif hasattr(self.__class__, method.name):
            return getattr(self.__class__, method.name)
        else:
            raise ImporterMethodNotFound('Importer "%s" has no method named "%s".' %
                                         (self.__class__.__name__, method.name))

import types
import six
import time
import logging
from copy import copy
from chomper.utils import smart_invoke
from chomper.exceptions import ItemNotImportable, ImporterMethodNotFound, DropItem
from chomper.items import Item


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

    def setup(self):
        def _setup(actions):
            for action in actions:
                if isinstance(action, list):
                    _setup(action)
                else:
                    action.set_importer(self)
        _setup(self.pipeline)

    def run(self):
        actions = copy(self.pipeline)
        root_action = actions.pop(0)
        result = self.invoke_action(root_action, [Item()])

        self.run_actions(result, actions)

        if not self.close_when_idle:
            time.sleep(1)
            self.run()
        else:
            # TODO: stats might need to be reset (if you call run() twice on the same instance)
            # TODO: need to handle closing pipeline actions here (eg. close database connections)
            self.logger.info('Importer finished, %d items were imported and %d were dropped' %
                             (self.items_processed, self.items_dropped))

    def run_actions(self, result, actions):
        # TODO: Refactor all of this; way to messy
        action = actions.pop(0)
        is_generator = isinstance(result, types.GeneratorType)
        is_list = isinstance(result, list) or isinstance(result, tuple)

        if not is_generator and not is_list:
            result = [result]

        for item in result:
            if item is None:
                continue

            # Execute child pipelines
            if isinstance(action, list):
                # Child pipelines don't return a result to the parent pipeline or
                # manipulate the item in parent pipeline
                next_result = copy(item)
                self.run_actions(item, copy(action))
            else:
                try:
                    next_result = self.invoke_action(action, [item])
                except (ItemNotImportable, DropItem) as e:
                    self.logger.info(e.message)
                    self.items_dropped += 1
                    continue

            if len(actions):
                self.run_actions(next_result, copy(actions))
            else:
                self.items_processed += 1

    def invoke_action(self, action, action_args):
        if callable(action):
            return smart_invoke(action, action_args)
        elif isinstance(action, six.string_types) and hasattr(self, action):
            return smart_invoke(getattr(self, action), action_args)
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

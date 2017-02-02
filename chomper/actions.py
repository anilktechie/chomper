import logging
import json
import six

from chomper.exceptions import ItemNotImportable
from chomper.utils import smart_invoke


class Action(object):
    """
    Base class for all pipeline actions
    """

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)


class Defaults(Action):
    """
    Set defaults fields and values on a item

    Sources can be list of dicts or callables. Sources are applied from eft to right.

    Defaults will only be set if the field has not yet been defined or is "None".
    All other falsy values will be skipped.
    """

    def __init__(self, *sources, **values):
        if sources:
            self.sources = sources
        elif values:
            self.sources = [values]
        else:
            raise ValueError('Default values were not provided.')

    def __call__(self, item, meta, importer):
        for source in self.sources:
            if callable(source):
                defaults = smart_invoke(source, [item, meta, importer])
            else:
                defaults = source

            if not defaults:
                continue

            if not isinstance(source, dict):
                raise ValueError('Cannot assign default values as the source in not a dict.')

            for key, default in six.iteritems(defaults):
                if key not in item or item[key] is None:
                    item[key] = default

        return item


class Assign(Action):
    """
    Define a field on an item and assign it's value

    Value can be cached if the cache flag is set to true (useful for database
    queries that will always return the same result for all items in a pipeline)
    """

    def __init__(self, field, func, cache=False):
        self.field = field
        self.func = func
        self.cache = cache
        self.result = None
        self.executed = False

    def __call__(self, item, meta, importer):
        if self.field in item:
            self.logger.debug('Assign action will override an existing value for key "%s"' % self.field)

        if self.cache and self.executed:
            item[self.field] = self.result
            return item
        else:
            self.executed = True
            self.result = self.invoke_func(item, meta, importer)
            item[self.field] = self.result
            return item

    def invoke_func(self, item, meta, importer):
        args = [item, meta, importer]
        if callable(self.func):
            return smart_invoke(self.func, args)
        elif isinstance(self.func, six.string_types) and hasattr(importer, self.func):
            return smart_invoke(getattr(importer, self.func), args)
        else:
            return self.func


class DropItem(Action):
    """
    Drop an item if the expression evaluates true
    """

    def __init__(self, expression):
        self.expression = expression

    def __call__(self, item):
        if item.eval(self.expression):
            raise ItemNotImportable('Item dropped as the provided expression "%s" evaluated to true.' % self.expression)
        return item


class DropField(Action):
    """
    Removes a single field from a item the the provided expression evaluates to true
    """

    def __init__(self, field, expression):
        self.field = field
        self.expression = expression

    def __call__(self, item):
        if item.eval(self.expression):
            try:
                del item[self.field]
            except KeyError:
                # Field has not been defined on the item
                pass
        return item


class FilterValue(Action):
    """
    Filter a single field value on an item

    Action is skipped if the field is not defined on the item
    """

    def __init__(self, field, filter_func):
        self.field = field
        self.filter = filter_func

        if not callable(self.filter):
            raise ValueError('Filter must be callable')

    def __call__(self, item):
        try:
            value = item[self.field]
            item[self.field] = smart_invoke(self.filter, [value, item])
        except KeyError:
            pass
        except TypeError:
            pass
        return item


class MapValues(Action):
    """
    Map the values of an item field
    """

    def __init__(self, field, mapping=None):
        self.field = field
        self.mapping = mapping

        if not self.mapping or not isinstance(mapping, dict):
            raise ValueError('Mapping argument must be a dict (key is current value, value is the new value)')

    def __call__(self, item):
        try:
            value = item[self.field]
            item[self.field] = self.mapping[value]
        except KeyError:
            pass
        return item


class MapKeys(Action):
    """
    Map the field keys on an item

    If the current key is not defined on the item the new key will not be defined
    """

    def __init__(self, mapping=None, **kwargs):
        if mapping is None:
            self.mapping = kwargs
        else:
            self.mapping = mapping

        if not self.mapping or not isinstance(self.mapping, dict):
            raise ValueError('Mapping argument must be a dict (key is current key name, value is the new key name)')

    def __call__(self, item):
        for current_key, new_key in six.iteritems(self.mapping):
            try:
                item[new_key] = item[current_key]
                del item[current_key]
            except KeyError:
                continue
        return item


class PickFields(Action):
    """
    Returns the item what only contains the provided fields
    """

    def __init__(self, *fields):
        self.fields = fields

    def __call__(self, item):
        for key in item.keys():
            if key not in self.fields:
                del item[key]
        return item


class OmitFields(Action):
    """
    Returns the item with the provided fields removed
    """

    def __init__(self, *fields):
        self.fields = fields

    def __call__(self, item):
        for key in item.keys():
            if key in self.fields:
                del item[key]
        return item


class LogItem(Action):

    def __init__(self, level=logging.DEBUG):
        self.level = level

    def __call__(self, item, meta):
        if isinstance(item, dict):
            self.logger.log(self.level, json.dumps(item, indent=4, sort_keys=True))
        else:
            self.logger.log(self.level, str(item))
        return item

import logging
import json
import six

from chomper.exceptions import ItemNotImportable, ImporterMethodNotFound
from chomper.utils import smart_invoke


class Processor(object):
    """
    Base class for all pipeline processors
    """

    def __init__(self, name=None, importer=None):
        self.name = name if name else self.__class__.__name__
        self.importer = importer

    def __call__(self, item):
        if not self.should_process(item):
            return item
        item = self.before_process(item)
        item = self.process(item)
        return self.after_process(item)

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)

    def get_importer(self):
        return self.importer

    def set_importer(self, importer):
        self.importer = importer

    def should_process(self, item):
        return True

    def before_process(self, item):
        return item

    def process(self, item):
        return item

    def after_process(self, item):
        return item

    def is_callable(self, func):
        from chomper.importers import ImporterMethod
        return callable(func) or isinstance(func, ImporterMethod)

    def invoke_func(self, func, args):
        from chomper.importers import ImporterMethod

        if isinstance(func, ImporterMethod):
            if not self.importer:
                raise ImporterMethodNotFound('Importer methods cannot be used on processors that are not '
                                             'associated with an importer.')
            func = self.importer.get_method(func)

        if not callable(func):
            raise ValueError('Filter must be callable')

        return smart_invoke(func, args)


class Defaulter(Processor):
    """
    Set default values for fields on an item.

    Defaults will only be set if the field has not yet been defined or is "None".
    All other falsy values will be skipped.
    """

    def __init__(self, defaults):
        super(Defaulter, self).__init__()
        if defaults:
            self.defaults = defaults
        else:
            raise ValueError('Default values were not provided or were an empty dict.')

    def process(self, item):
        if self.is_callable(self.defaults):
            defaults = self.invoke_func(self.defaults, [item])
        else:
            defaults = self.defaults

        if not isinstance(defaults, dict):
            raise ValueError('Cannot assign default values as the source in not a dict.')

        for key, default in six.iteritems(defaults):
            if key not in item or item[key] is None:
                item[key] = default

        return item


class Assigner(Processor):
    """
    Define a field on an item and assign it's value

    Value can be either static or a callable
    """

    def __init__(self, field, value):
        super(Assigner, self).__init__()
        self.field = field
        self.value = value

    def process(self, item):
        if self.field in item:
            self.logger.debug('Assign action will override an existing value for key "%s"' % self.field)

        if self.is_callable(self.value):
            item[self.field] = self.invoke_func(self.value, [item])
        else:
            item[self.field] = self.value

        return item


class ItemDropper(Processor):
    """
    Drop an item if the expression evaluates true
    """

    def __init__(self, expression):
        super(ItemDropper, self).__init__()
        self.expression = expression

    def process(self, item):
        if item.eval(self.expression):
            raise ItemNotImportable('Item dropped as the provided expression "%s" evaluated to true.' % self.expression)
        return item


class FieldDropper(Processor):
    """
    Removes a single field from a item the the provided expression evaluates to true
    """

    def __init__(self, field, expression):
        super(FieldDropper, self).__init__()
        self.field = field
        self.expression = expression

    def process(self, item):
        if item.eval(self.expression):
            try:
                del item[self.field]
            except KeyError:
                # Field has not been defined on the item
                pass
        return item


class ValueFilter(Processor):
    """
    Filter a single field value on an item

    Action is skipped if the field is not defined on the item
    """

    def __init__(self, field, filter_func):
        super(ValueFilter, self).__init__()
        self.field = field
        self.filter = filter_func

    def process(self, item):
        try:
            value = item[self.field]
            item[self.field] = self.invoke_func(self.filter, [value, item])
        except KeyError:
            self.logger.warn('Could not filter value as the field did not exist on the item.')
        except TypeError:
            self.logger.warn('Could not call filter as it was not a valid callable.')

        return item


class ValueMapper(Processor):
    """
    Map the values of an item field

    Mapping must be a dict (or a callable that returns one)
    Mapping dict keys are the search value; mapping values are the replacement
    """

    def __init__(self, field, mapping):
        super(ValueMapper, self).__init__()
        self.field = field
        self.mapping = mapping

        if not isinstance(mapping, dict) and not callable(mapping):
            raise ValueError('Mapping argument must be a dict or a callable.')

    def process(self, item):
        if self.is_callable(self.mapping):
            mapping = self.invoke_func(self.mapping, [item])
        else:
            mapping = self.mapping

        try:
            value = item[self.field]
            item[self.field] = mapping[value]
        except KeyError:
            pass
        return item


class KeyMapper(Processor):
    """
    Map the field keys on an item

    If the current key is not defined on the item the new key will not be defined
    """

    def __init__(self, mapping):
        super(KeyMapper, self).__init__()
        self.mapping = mapping

        if not isinstance(mapping, dict) and not callable(mapping):
            raise ValueError('Mapping argument must be a dict or a callable that returns one.')

    def process(self, item):
        if self.is_callable(self.mapping):
            mapping = self.invoke_func(self.mapping, [item])
        else:
            mapping = self.mapping

        for current_key, new_key in six.iteritems(mapping):
            try:
                item[new_key] = item[current_key]
                del item[current_key]
            except KeyError:
                continue
        return item


class FieldPicker(Processor):
    """
    Returns the item what only contains the provided fields
    """

    def __init__(self, *fields):
        super(FieldPicker, self).__init__()
        self.fields = fields

    def process(self, item):
        from chomper.items import Field

        field_keys = []
        for field in self.fields:
            if isinstance(field, Field):
                field_keys.append(field.get_name())
            elif isinstance(field, six.string_types):
                field_keys.append(field)

        for key in item.keys():
            if key not in field_keys:
                del item[key]

        return item


class FieldOmitter(Processor):
    """
    Returns the item with the provided fields removed
    """

    def __init__(self, *fields):
        super(FieldOmitter, self).__init__()
        self.fields = fields

    def process(self, item):
        from chomper.items import Field

        field_keys = []
        for field in self.fields:
            if isinstance(field, Field):
                field_keys.append(field.get_name())
            elif isinstance(field, six.string_types):
                field_keys.append(field)

        for key in item.keys():
            if key in field_keys:
                del item[key]

        return item


class Logger(Processor):

    def __init__(self, level=logging.DEBUG):
        super(Logger, self).__init__()
        self.level = level

    def __call__(self, item):
        self.logger.log(self.level, json.dumps(item, indent=4, sort_keys=True))
        return item

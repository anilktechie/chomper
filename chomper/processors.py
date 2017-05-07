from pydash import defaults, defaults_deep

from chomper import Node, processor
from chomper.support import generative


class Defaults(Node):
    """
    Set default values on an item.

    Defaults will only be set if the field has not yet been defined or is "None".
    All other falsy values will be skipped.
    """

    def __init__(self, name):
        self._defaults = None
        self._deep = False
        super(Defaults, self).__init__(name)

    @generative
    def value(self, default_value):
        self._defaults = default_value

    values = value

    @generative
    def deep(self, is_deep=True):
        self._deep = is_deep

    @processor(['dict'])
    def process_dict(self, item):
        if not isinstance(self._defaults, dict):
            raise TypeError('Setting default values on a dict requires '
                            'the defaults to also be a dict.')

        if item is None:
            item = self._defaults
        elif self._deep:
            item = defaults_deep(item, self._defaults)
        else:
            item = defaults(item, self._defaults)

        self.push(item)

    @processor()
    def process_other(self, item):
        self.push(item if item is not None else self._defaults)


class Set(Node):

    def __init__(self, name):
        self._value = None
        super(Set, self).__init__(name)

    @generative
    def value(self, value):
        self._value = value

    @processor()
    def filter_value(self, item):
        try:
            item = self._value(item)
        except TypeError:
            item = self._value
        finally:
            self.push(item)

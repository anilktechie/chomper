import inspect
import six
from copy import copy

from chomper.utils import iter_methods


def is_chainable(func):
    lines = inspect.getsourcelines(func)
    return lines and 'return self' in lines[0][-1]


class Signature(object):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = list(args)
        self.kwargs = kwargs

    def __repr__(self):
        return 'Signature(%s, %s, %s)' % (self.name, self.args, self.kwargs)

    def __copy__(self):
        return self.__class__(self.name, *copy(self.args), **copy(self.kwargs))


class ExecutionContext(object):

    def __init__(self, _values=None, **values):
        if _values is not None:
            values.update(_values)

        for k, v in six.iteritems(values):
            setattr(self, k, v)


class Interceptor(object):

    def enable_execution(self):
        self._executing = True

    def disable_execution(self):
        self._executing = False

    def set_execution_context(self, _values=None, **values):
        args = dict()

        if _values and isinstance(_values, dict):
            args.update(_values)

        if values:
            args.update(values)

        self._execution_context = ExecutionContext(args)

    def flush_signatures(self):
        self._signatures = []

    def execute(self, base=None, _context=None, **context):
        obj = base if base is not None else copy(self)

        if _context is not None:
            context.update(_context)

        self.set_execution_context(context)

        for signature in self._signatures:
            signature = copy(signature)
            signature = self.filter_signature(signature, self._execution_context)
            func = getattr(self, signature.name)
            obj = func(*signature.args, **signature.kwargs)

        return obj

    def filter_signature(self, signature, context):
        return signature


class SignatureInterceptor(type):

    def __new__(mcs, name, bases, attr):
        bases += (Interceptor,)
        return super(SignatureInterceptor, mcs).__new__(mcs, name, bases, attr)

    def __init__(cls, name, bases, attr):
        cls.apply_init_wrapper(cls)
        cls.apply_wrappers(cls)
        super(SignatureInterceptor, cls).__init__(name, bases, attr)

    @classmethod
    def apply_init_wrapper(mcs, cls):
        old_init = cls.__init__

        def __init__(self, *args, **kwargs):
            self._signatures = []
            self._executing = False
            self._execution_context = dict()
            old_init(self, *args, **kwargs)

        cls.__init__ = __init__

    @classmethod
    def apply_wrappers(mcs, cls):
        for name, method in iter_methods(cls):
            if name in mcs.get_wrappable_methods(cls):
                setattr(cls, name, mcs.apply_wrapper(name, method))

    @classmethod
    def apply_wrapper(mcs, name, method):
        def record(self, *args, **kwargs):
            if not self._executing:
                self._signatures.append(Signature(name, *args, **kwargs))
                return self
            else:
                return method(self, *args, **kwargs)
        return record

    @classmethod
    def get_wrappable_methods(mcs, cls):
        try:
            intercepts = cls.__intercept__
            if callable(intercepts):
                intercepts = intercepts()
            if isinstance(intercepts, six.string_types):
                intercepts = [intercepts]
            return intercepts
        except AttributeError:
            return []

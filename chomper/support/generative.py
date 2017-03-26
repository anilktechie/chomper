from functools import wraps


class GenerativeBase(object):

    def _generate(self):
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        return s


def generative(func):
    @wraps(func)
    def decorator(self, *args, **kw):
        new_self = self._generate()
        func(new_self, *args, **kw)
        return new_self
    return decorator

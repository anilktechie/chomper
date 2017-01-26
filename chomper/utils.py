import inspect


class AttrDict(dict):
    """
    Dict-like object to allow accessing keys as attributes

    Note: Causes a memory leak in Python < 2.7.4 and Python3 < 3.2.3
    http://bugs.python.org/issue1469629
    """

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def smart_invoke(func, args):
    """
    Invoke the provided function / callable with the correct number of arguments
    """
    if not (isinstance(args, list) or isinstance(args, tuple)):
        args = [args]

    try:
        spec = inspect.getargspec(func)
    except TypeError:
        # inspect.getargspec does not support callable objects be default
        if hasattr(func, '__call__'):
            spec = inspect.getargspec(func.__call__)
        else:
            raise

    # Remove the "self" / "cls" arg from the spec
    spec_args = [arg for arg in spec.args if arg not in ['self', 'cls']]

    return func(*args[:len(spec_args)])

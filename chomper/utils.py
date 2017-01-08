import inspect


def smart_invoke(func, args):
    """
    Invoke the provided function / callable with the correct number of arguments
    """
    if not (isinstance(args, list) or isinstance(args, tuple)):
        args = [args]

    # inspect.getargspec does not support callable objects be default
    if hasattr(func, '__call__'):
        func = func.__call__

    spec = inspect.getargspec(func)

    # If the func is a method invoke with one less arg to allow for self
    if inspect.ismethod(func):
        arg_count = len(spec.args) - 1
    else:
        arg_count = len(spec.args)

    return func(*args[:arg_count])

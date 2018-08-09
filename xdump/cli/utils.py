

def apply_decorators(decorators):
    """Apply multiple decorators to the same function. Useful for reusing common decorators among many functions."""

    def wrapper(func):
        for decorator in reversed(decorators):
            func = decorator(func)
        return func

    return wrapper


def import_backend(path):
    """Import a backend class by dotted path."""
    module_name, class_name = path.rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    return getattr(module, class_name)

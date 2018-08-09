

def apply_decorators(decorators):

    def wrapper(func):
        for decorator in reversed(decorators):
            func = decorator(func)
        return func

    return wrapper


def import_backend(path):
    module_name, class_name = path.rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    return getattr(module, class_name)

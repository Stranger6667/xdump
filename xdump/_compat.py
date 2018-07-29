import sys


if sys.version_info[0] == 2:
    from StringIO import StringIO
else:
    from io import StringIO  # noqa

try:
    FileNotFoundError = FileNotFoundError
except NameError:
    FileNotFoundError = OSError

try:
    from functools import lru_cache
except ImportError:
    from repoze.lru import lru_cache as cache

    def lru_cache():

        def wrapper(f):
            wrapped = cache(None)(f)
            wrapped.cache_clear = lambda: wrapped._cache.clear()
            return wrapped

        return wrapper

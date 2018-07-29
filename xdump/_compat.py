import sys


if sys.version_info[0] == 2:
    from StringIO import StringIO
else:
    from io import StringIO

try:
    FileNotFoundError = FileNotFoundError
except NameError:
    FileNotFoundError = OSError

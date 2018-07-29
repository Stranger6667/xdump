.. _changelog:

Changelog
=========

`Unreleased`_
-------------

Added
~~~~~

- Options to exclude schema or data from the dump. `#29`_
- PyPy 3 support

Changed
~~~~~~~

- SQLite < 3.8.3 is unsupported explicitly. ``RuntimeError`` is raised with that SQLite version. `#13`_

`0.4.0`_ - 2018-07-23
---------------------

Added
~~~~~

- Dump compression level customization. `#16`_
- Logging support. `#15`_

Fixed
~~~~~

- Querying for relations by read-only user in Postgres. `#21`_
- Constraint names clashing on different tables. `#22`_

`0.3.1`_ - 2018-06-19
---------------------

Fixed
~~~~~

- Postgresql search path issue

`0.3.0`_ - 2018-03-13
---------------------

Added
~~~~~

- Support for Python 3.4 & 3.5. `#7`_
- Automatic selection of related objects.

`0.2.0`_ - 2018-01-07
---------------------

Added
~~~~~

- SQLite support. `#8`_

`0.1.2`_ - 2018-01-03
---------------------

Fixed
~~~~~

- Changed user for Django integration.

`0.1.1`_ - 2018-01-03
---------------------

Fixed
~~~~~

- Packaging issue.

0.1.0 - 2018-01-03
------------------

- Initial release.

.. _Unreleased: https://github.com/Stranger6667/xdump/compare/0.4.0...HEAD
.. _0.4.0: https://github.com/Stranger6667/xdump/compare/0.3.1...0.4.0
.. _0.3.1: https://github.com/Stranger6667/xdump/compare/0.3.0...0.3.1
.. _0.3.0: https://github.com/Stranger6667/xdump/compare/0.2.0...0.3.0
.. _0.2.0: https://github.com/Stranger6667/xdump/compare/0.1.2...0.2.0
.. _0.1.2: https://github.com/Stranger6667/xdump/compare/0.1.1...0.1.2
.. _0.1.1: https://github.com/Stranger6667/xdump/compare/0.1.0...0.1.1

.. _#29: https://github.com/Stranger6667/xdump/issues/29
.. _#22: https://github.com/Stranger6667/xdump/issues/22
.. _#21: https://github.com/Stranger6667/xdump/issues/21
.. _#16: https://github.com/Stranger6667/xdump/issues/16
.. _#15: https://github.com/Stranger6667/xdump/issues/15
.. _#13: https://github.com/Stranger6667/xdump/issues/13
.. _#8: https://github.com/Stranger6667/xdump/issues/8
.. _#7: https://github.com/Stranger6667/xdump/issues/7

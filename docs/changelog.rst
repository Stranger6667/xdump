.. _changelog:

Changelog
=========

`Unreleased`_
-------------

Changed
~~~~~~~

- Required version of click package

`0.6.0`_ - 2018-08-11
---------------------

Added
~~~~~

- Possibility to truncate data in the database instead of the DB re-creation. ``truncate`` method in DB backends. `#48`_
- Command Line Interface. `#6`_

Changed
~~~~~~~

- Do not try to load schema if it is absent in the dump. `#39`_
- ``xdump.sqlite.SQLiteBackend`` now accepts only ``dbname`` and ``verbosity``. `#52`_
- Now Django's ``xload`` command doesn't recreate the DB but provides a non required option ``--cleanup-method`` for
  choosing how to clean-up the database before the dump loading. `#53`_
- Django's ``xload`` command options are renamed.
  ``--dump_data`` -> ``--dump-data`` and ``--dump_schema`` -> ``--dump-schema``.

`0.5.0`_ - 2018-08-02
---------------------

Added
~~~~~

- Options to exclude schema or data from the dump. `#29`_
- PyPy 2 & 3 and Python 2.7 support.

Changed
~~~~~~~

- SQLite < 3.8.3 is unsupported explicitly. ``RuntimeError`` is raised with unsupported SQLite version. `#13`_
- Relation's data loading is optimized. `#25`_
- Relation's query for PostgreSQL is optimized. `#45`_
- An error is raised if a key from ``partial_tables`` is contained by ``full_tables``. `#44`_

`0.4.0`_ - 2018-07-23
---------------------

Added
~~~~~

- Dump compression level customization. `#16`_
- Logging support. `#15`_

Fixed
~~~~~

- Querying for relations by a read-only user in PostgreSQL. `#21`_
- Constraint names were clashing on different tables. `#22`_

`0.3.1`_ - 2018-06-19
---------------------

Fixed
~~~~~

- PostgreSQL search path issue.

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

.. _Unreleased: https://github.com/Stranger6667/xdump/compare/0.6.0...HEAD
.. _0.6.0: https://github.com/Stranger6667/xdump/compare/0.5.0...0.6.0
.. _0.5.0: https://github.com/Stranger6667/xdump/compare/0.4.0...0.5.0
.. _0.4.0: https://github.com/Stranger6667/xdump/compare/0.3.1...0.4.0
.. _0.3.1: https://github.com/Stranger6667/xdump/compare/0.3.0...0.3.1
.. _0.3.0: https://github.com/Stranger6667/xdump/compare/0.2.0...0.3.0
.. _0.2.0: https://github.com/Stranger6667/xdump/compare/0.1.2...0.2.0
.. _0.1.2: https://github.com/Stranger6667/xdump/compare/0.1.1...0.1.2
.. _0.1.1: https://github.com/Stranger6667/xdump/compare/0.1.0...0.1.1

.. _#53: https://github.com/Stranger6667/xdump/issues/53
.. _#52: https://github.com/Stranger6667/xdump/issues/52
.. _#48: https://github.com/Stranger6667/xdump/issues/48
.. _#45: https://github.com/Stranger6667/xdump/issues/45
.. _#44: https://github.com/Stranger6667/xdump/issues/44
.. _#39: https://github.com/Stranger6667/xdump/issues/39
.. _#29: https://github.com/Stranger6667/xdump/issues/29
.. _#25: https://github.com/Stranger6667/xdump/issues/25
.. _#22: https://github.com/Stranger6667/xdump/issues/22
.. _#21: https://github.com/Stranger6667/xdump/issues/21
.. _#16: https://github.com/Stranger6667/xdump/issues/16
.. _#15: https://github.com/Stranger6667/xdump/issues/15
.. _#13: https://github.com/Stranger6667/xdump/issues/13
.. _#8: https://github.com/Stranger6667/xdump/issues/8
.. _#7: https://github.com/Stranger6667/xdump/issues/7
.. _#6: https://github.com/Stranger6667/xdump/issues/6

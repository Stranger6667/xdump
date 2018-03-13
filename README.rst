XDump
=====

.. image:: https://travis-ci.org/Stranger6667/xdump.svg?branch=master
   :target: https://travis-ci.org/Stranger6667/xdump
   :alt: Build Status

.. image:: https://codecov.io/github/Stranger6667/xdump/coverage.svg?branch=master
   :target: https://codecov.io/github/Stranger6667/xdump?branch=master
   :alt: Coverage Status

.. image:: https://readthedocs.org/projects/xdump/badge/?version=stable
   :target: http://xdump.readthedocs.io/en/stable/?badge=stable
   :alt: Documentation Status

.. image:: https://img.shields.io/pypi/v/xdump.svg
    :target: https://pypi.python.org/pypi/xdump
    :alt: Latest PyPI version

XDump is an utility to make partial consistent dump and load it into the database.

The idea is to provide an ability to specify what to include in the dump via SQL queries.

Installation
============

XDump can be obtained with ``pip``::

    $ pip install xdump

Usage example
=============

Making a dump (on production replica for example):

.. code-block:: python

    >>> EMPLOYEES_SQL = 'SELECT * FROM employees ORDER BY id DESC LIMIT 2'
    >>> from xdump.postgresql import PostgreSQLBackend
    >>> backend = PostgreSQLBackend(dbname='app_db', user='prod', password='pass', host='127.0.0.1', port='5432')
    >>> backend.dump('/path/to/dump.zip', full_tables=['groups'], partial_tables={'employees': EMPLOYEES_SQL})

Automatic selection of related objects
++++++++++++++++++++++++++++++++++++++

You don't have to specify all queries for related objects - XDump will load them for you automatically. It covers
both, recursive and non-recursive relations.
For example, if the ``employees`` table has foreign keys ``group_id`` (to ``groups`` table) and ``manager_id``
(to ``employees`` table) the resulting dump will have all objects related to selected employees
(as well as for objects related to related objects, recursively).

Load a dump on you local machine:

.. code-block:: python

    from xdump.postgresql import PostgreSQLBackend
    >>> backend = PostgreSQLBackend(dbname='app_db', user='local', password='pass', host='127.0.0.1', port='5432')
    >>> backend.load('/path/to/dump.zip')

RDBMS support
=============

At the moment only the following are supported:

- PostgreSQL
- SQLite

Django support
==============

Add ``xdump.extra.django`` to your ``INSTALLED_APPS`` settings:

.. code-block:: python

    INSTALLED_APPS = [
       ...,
       'xdump.extra.django',
    ]

Add ``XDUMP`` to your project settings file. It should contain minimum 2 entries:

- FULL_TABLES - a list of tables, that should be fully dumped.
- PARTIAL_TABLES - a dictionary with ``table_name``: ``select SQL``

.. code-block:: python

    XDUMP = {
        'FULL_TABLES': ['groups'],
        'PARTIAL_TABLES': {'employees': 'SELECT * FROM employees WHERE id > 100'}
    }


Optionally you could use a custom backend:

.. code-block:: python

    XDUMP = {
        ...,
        'BACKEND': 'importable.string',
    }


Run ``xdump`` command::

    $ ./manage.py xdump dump.zip


Run ``xload`` command::

    $ ./manage.py xload dump.zip

Possible options to both commands:

- ``alias`` - allows you to choose database config from DATABASES, that is used during the execution;
- ``backend`` - importable string, that leads to custom dump backend class.

The following ``make`` command could be useful to get a configured dump from production to your local machine:

.. code-block:: bash

    sync-production:
        ssh -t $(TARGET) "DJANGO_SETTINGS_MODULE=settings.production /path/to/manage.py xdump /tmp/dump.zip"
        scp $(TARGET):/tmp/dump.zip ./dump.zip
        ssh -t $(TARGET) "rm /tmp/dump.zip"
        DJANGO_SETTINGS_MODULE=settings.local $(PYTHON) manage.py xload ./dump.zip

And usage is:

.. code-block:: bash

    $ make sync-production TARGET=john@production.com PYTHON=/path/to/python/in/venv


Python support
==============

XDump supports Python 3.4, 3.5 & 3.6.

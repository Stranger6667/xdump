XDump
=====

.. image:: https://travis-ci.org/Stranger6667/xdump.svg?branch=master
   :target: https://travis-ci.org/Stranger6667/xdump
   :alt: Build Status

.. image:: https://codecov.io/github/Stranger6667/xdump/coverage.svg?branch=master
   :target: https://codecov.io/github/Stranger6667/xdump?branch=master
   :alt: Coverage Status

.. image:: https://img.shields.io/pypi/v/xdump.svg
    :target: https://pypi.python.org/pypi/xdump
    :alt: Latest PyPI version

XDump is an utility to make partial consistent dump and load it into the database.

The idea is to provide an ability to specify what to include in the dump via SQL queries.
Responsibility for dump integrity lays on the user in this case.

Installation
============

XDump can be obtained with ``pip``::

    $ pip install xdump

Usage example
=============

Making a dump (on production replica for example):

.. code-block:: python

    >>> EMPLOYEES_SQL = '''
    WITH RECURSIVE employees_cte AS (
      SELECT *
      FROM recent_employees
      UNION
      SELECT E.*
      FROM employees E
      INNER JOIN employees_cte ON (employees_cte.manager_id = E.id)
    ), recent_employees AS (
      SELECT *
      FROM employees
      ORDER BY id DESC
      LIMIT 2
    )
    SELECT * FROM employees_cte
    '''
    >>> from xdump.postgresql import PostgreSQLBackend
    >>> backend = PostgreSQLBackend(dbname='app_db', user='prod', password='pass', host='127.0.0.1', port='5432')
    >>> backend.dump('/path/to/dump.zip', full_tables=['groups'], partial_tables={'employees': EMPLOYEES_SQL})


Load a dump on you local machine:

.. code-block:: python

    from xdump.postgresql import PostgreSQLBackend
    >>> backend = PostgreSQLBackend(dbname='app_db', user='local', password='pass', host='127.0.0.1', port='5432')
    >>> backend.load('/path/to/dump.zip')

RDBMS support
=============

At the moment only the following are supported:

- PostgreSQL

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

XDump supports only Python 3.6 at the moment (but the supported versions list will be extended in the future).

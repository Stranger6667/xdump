# XDump

[![Build](https://github.com/Stranger6667/xdump/actions/workflows/build.yml/badge.svg)](https://github.com/Stranger6667/xdump/actions/workflows/build.yml)
[![Coverage](https://codecov.io/github/Stranger6667/xdump/coverage.svg?branch=master)](https://codecov.io/github/Stranger6667/xdump?branch=master)
[![Version](https://img.shields.io/pypi/v/xdump.svg)](https://pypi.org/project/xdump/)
[![Python versions](https://img.shields.io/pypi/pyversions/xdump.svg)](https://pypi.org/project/xdump/)
[![License](https://img.shields.io/pypi/l/xdump.svg)](https://opensource.org/licenses/MIT)

XDump is a utility to make a consistent partial dump and load it into the database.

The idea is to provide an ability to specify what to include in the dump via SQL queries.

## Installation

```console
$ pip install xdump
```

## Usage example

Make a dump (on production replica for example):

```python
from xdump.postgresql import PostgreSQLBackend

backend = PostgreSQLBackend(dbname="app_db", user="prod", password="pass", host="127.0.0.1", port="5432")
backend.dump(
    "/path/to/dump.zip",
    full_tables=["groups"],
    partial_tables={"employees": "SELECT * FROM employees ORDER BY id DESC LIMIT 2"},
)
```

Load a dump on your local machine:

```python
backend = PostgreSQLBackend(dbname="app_db", user="local", password="pass", host="127.0.0.1", port="5432")
# If you need a clear DB
backend.recreate_database()  # or `backend.truncate()`
backend.load("/path/to/dump.zip")
```

Dump is compressed by default. Compression level could be changed with passing `compression` argument to `dump` method.
Valid options are `zipfile.ZIP_STORED`, `zipfile.ZIP_DEFLATED`, `zipfile.ZIP_BZIP2` and `zipfile.ZIP_LZMA`.

The verbosity of the output could be customized via `verbosity` (with values 0, 1 or 2) argument of a backend class.

There are two options to control the content of the dump:

- `dump_schema` - controls if the schema should be included
- `dump_data` - controls if the data should be included

### Automatic selection of related objects

You don't have to specify all queries for related objects - XDump will load them for you automatically. It covers
both, recursive and non-recursive relations.
For example, if the `employees` table has foreign keys `group_id` (to `groups` table) and `manager_id`
(to `employees` table) the resulting dump will have all objects related to selected employees
(as well as for objects related to related objects, recursively).

## Command Line Interface

`xdump` creates a dump.

```console
$ xdump [postgres|sqlite] [OPTIONS]
```

Common options:

```
-o, --output TEXT               output file name  [required]
-f, --full TEXT                 table name to be fully dumped. Could be used
                                multiple times
-p, --partial TEXT              partial tables specification in a form
                                "table_name:select SQL". Could be used
                                multiple times
-c, --compression [deflated|stored|bzip2|lzma]
                                dump compression level
--schema / --no-schema          include / exclude the schema from the dump
--data / --no-data              include / exclude the data from the dump
-D, --dbname TEXT               database to work with  [required]
-v, --verbosity                 verbosity level
```

PostgreSQL-specific options:

```
-U, --user TEXT                 connect as specified database user
                                [required]
-W, --password TEXT             password for the DB connection
-H, --host TEXT                 database server host or socket directory
-P, --port TEXT                 database server port number
```

`xload` loads a dump into a database.

```console
$ xload [postgres|sqlite] [OPTIONS]
```

Common options:

```
-i, --input TEXT                input file name  [required]
-m, --cleanup-method [recreate|truncate]
                                method of DB cleaning up
-D, --dbname TEXT               database to work with  [required]
-v, --verbosity                 verbosity level
```

PostgreSQL-specific options are the same as for `xdump`.

## RDBMS support

At the moment only the following are supported:

- PostgreSQL
- SQLite >= 3.8.3

`pg_dump` and the `sqlite3` command-line tool should be available in `PATH`.

## Django support

Add `xdump.extra.django` to your `INSTALLED_APPS` settings:

```python
INSTALLED_APPS = [
    ...,
    "xdump.extra.django",
]
```

Add `XDUMP` to your project settings file. It should contain minimum two entries:

- `FULL_TABLES` - a list of tables that should be fully dumped.
- `PARTIAL_TABLES` - a dictionary with `table_name`: `select SQL`

```python
XDUMP = {
    "FULL_TABLES": ["groups"],
    "PARTIAL_TABLES": {"employees": "SELECT * FROM employees WHERE id > 100"},
}
```

Optionally you could use a custom backend:

```python
XDUMP = {
    ...,
    "BACKEND": "importable.string",
}
```

Run `xdump` command:

```console
$ ./manage.py xdump dump.zip
```

Run `xload` command:

```console
$ ./manage.py xload dump.zip
```

Possible options to both commands:

- `-a/--alias` - allows you to choose database config from `DATABASES`, that is used during the execution;
- `-b/--backend` - importable string, that leads to custom dump backend class.

Options for `xdump` command:

- `-s/--dump-schema` - controls if the schema should be included;
- `-d/--dump-data` - controls if the data should be included.

Options for `xload` command:

- `-m/--cleanup-method` - optionally re-creates DB or truncates the data.

**NOTE**. If the dump has no schema inside, DB won't be re-created.

The following `make` command could be useful to get a configured dump from production to your local machine:

```make
sync-production:
	ssh -t $(TARGET) "DJANGO_SETTINGS_MODULE=settings.production /path/to/manage.py xdump /tmp/dump.zip"
	scp $(TARGET):/tmp/dump.zip ./dump.zip
	ssh -t $(TARGET) "rm /tmp/dump.zip"
	DJANGO_SETTINGS_MODULE=settings.local $(PYTHON) manage.py xload ./dump.zip
```

And the usage is:

```console
$ make sync-production TARGET=john@production.com PYTHON=/path/to/python/in/venv
```

## Python support

XDump supports Python 3.11–3.15 and PyPy.

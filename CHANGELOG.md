# Changelog

## [Unreleased](https://github.com/Stranger6667/xdump/compare/0.6.0...HEAD)

### Added

- Support for Python 3.11–3.15.
- `py.typed` marker.

### Changed

- Use `psycopg` 3 instead of `psycopg2`. `libpq` is loaded at runtime, so installing XDump no longer needs `pg_config` and a C compiler.
- Minimum supported versions: `click` 8.2, `psycopg` 3.2 and `Django` 5.2.
- `cache_clear` closes the backend's open connections and no longer affects other backend instances.

### Fixed

- `ImportError` on import of `xdump.sqlite` on Python 3.
- Loading PostgreSQL dumps made by `pg_dump` 13.22, 14.19, 15.14, 16.10, 17.6 and newer, which contain `\restrict` meta-commands.
- Unclosed database connections after CLI and Django commands.
- Detecting the SQLite backend in Django commands. The `django.db.backends.sqlite3` engine raised `KeyError`.

### Removed

- Support for Python 2.7, 3.4–3.10 and PyPy 2.
- Dependency on `attrs`. Backends are dataclasses now.
- `PostgreSQLBackend.copy_expert` and `handle_run_exception` methods.

## [0.6.0](https://github.com/Stranger6667/xdump/compare/0.5.0...0.6.0) - 2018-08-11

### Added

- Possibility to truncate data in the database instead of the DB re-creation. `truncate` method in DB backends. [#48](https://github.com/Stranger6667/xdump/issues/48)
- Command Line Interface. [#6](https://github.com/Stranger6667/xdump/issues/6)

### Changed

- Do not try to load schema if it is absent in the dump. [#39](https://github.com/Stranger6667/xdump/issues/39)
- `xdump.sqlite.SQLiteBackend` now accepts only `dbname` and `verbosity`. [#52](https://github.com/Stranger6667/xdump/issues/52)
- Now Django's `xload` command doesn't recreate the DB but provides a non required option `--cleanup-method` for choosing how to clean-up the database before the dump loading. [#53](https://github.com/Stranger6667/xdump/issues/53)
- Django's `xload` command options are renamed. `--dump_data` -> `--dump-data` and `--dump_schema` -> `--dump-schema`.

## [0.5.0](https://github.com/Stranger6667/xdump/compare/0.4.0...0.5.0) - 2018-08-02

### Added

- Options to exclude schema or data from the dump. [#29](https://github.com/Stranger6667/xdump/issues/29)
- PyPy 2 & 3 and Python 2.7 support.

### Changed

- SQLite < 3.8.3 is unsupported explicitly. `RuntimeError` is raised with unsupported SQLite version. [#13](https://github.com/Stranger6667/xdump/issues/13)
- Relation's data loading is optimized. [#25](https://github.com/Stranger6667/xdump/issues/25)
- Relation's query for PostgreSQL is optimized. [#45](https://github.com/Stranger6667/xdump/issues/45)
- An error is raised if a key from `partial_tables` is contained by `full_tables`. [#44](https://github.com/Stranger6667/xdump/issues/44)

## [0.4.0](https://github.com/Stranger6667/xdump/compare/0.3.1...0.4.0) - 2018-07-23

### Added

- Dump compression level customization. [#16](https://github.com/Stranger6667/xdump/issues/16)
- Logging support. [#15](https://github.com/Stranger6667/xdump/issues/15)

### Fixed

- Querying for relations by a read-only user in PostgreSQL. [#21](https://github.com/Stranger6667/xdump/issues/21)
- Constraint names were clashing on different tables. [#22](https://github.com/Stranger6667/xdump/issues/22)

## [0.3.1](https://github.com/Stranger6667/xdump/compare/0.3.0...0.3.1) - 2018-06-19

### Fixed

- PostgreSQL search path issue.

## [0.3.0](https://github.com/Stranger6667/xdump/compare/0.2.0...0.3.0) - 2018-03-13

### Added

- Support for Python 3.4 & 3.5. [#7](https://github.com/Stranger6667/xdump/issues/7)
- Automatic selection of related objects.

## [0.2.0](https://github.com/Stranger6667/xdump/compare/0.1.2...0.2.0) - 2018-01-07

### Added

- SQLite support. [#8](https://github.com/Stranger6667/xdump/issues/8)

## [0.1.2](https://github.com/Stranger6667/xdump/compare/0.1.1...0.1.2) - 2018-01-03

### Fixed

- Changed user for Django integration.

## [0.1.1](https://github.com/Stranger6667/xdump/compare/0.1.0...0.1.1) - 2018-01-03

### Fixed

- Packaging issue.

## 0.1.0 - 2018-01-03

- Initial release.

# coding: utf-8
import os
import zipfile
from contextlib import contextmanager
from time import time

from ._compat import lru_cache
from .logging import get_logger


class BaseBackend(object):
    dbname = None
    connections = {'default': {}}
    schema_filename = 'dump/schema.sql'
    initial_setup_files = (schema_filename, )
    data_dir = 'dump/data/'

    @property
    def logger(self):
        if not hasattr(self, '_logger'):
            self._logger = get_logger('XDump', self.verbosity)
        return self._logger

    @contextmanager
    def log_query(self, sql, params=None):
        self.logger.debug('Execute query: %s' % sql)
        self.logger.debug('Parameters: %s' % str(params))
        with self.log_time():
            yield

    @contextmanager
    def log_time(self, message='Execution time: %s'):
        start = time()
        yield
        self.logger.debug(message % (time() - start))

    # Connection

    @lru_cache()
    def get_connection(self, name='default'):
        return self.connect(**self.connections[name])

    @lru_cache()
    def get_cursor(self, name='default'):
        return self.get_connection(name).cursor()

    def connect(self, *args, **kwargs):
        """
        Create a connection to the database.
        """
        raise NotImplementedError

    def get_connection_kwargs(self, **kwargs):
        for option in ('dbname', 'user', 'password', 'host', 'port'):
            kwargs.setdefault(option, getattr(self, option))
        return kwargs

    def cache_clear(self):
        self.get_cursor.cache_clear()
        self.get_connection.cache_clear()

    # Low-level commands executors

    def run_dump(self, *args, **kwargs):
        """
        Runs built-in utility for data / schema dumping.
        """
        raise NotImplementedError

    def run(self, sql, params=None, using='default'):
        with self.log_query(sql, params):
            cursor = self.get_cursor(using)
            cursor.execute(sql, params)
            try:
                return cursor.fetchall()
            except Exception as exc:
                self.handle_run_exception(exc)

    def handle_run_exception(self, exc):
        raise NotImplementedError

    @contextmanager
    def transaction(self):
        """
        Runs block of code inside a transaction.
        """
        self.run('BEGIN')
        yield
        self.run('COMMIT')

    # Dumping the data

    def dump(self, filename, full_tables=(), partial_tables=None, compression=zipfile.ZIP_DEFLATED, dump_schema=True,
             dump_data=True):
        """
        Creates a dump, which could be used to restore the database.
        """
        self.input_check(full_tables, partial_tables)
        with self.log_time('Total execution time: %s'):
            partial_tables = partial_tables or {}
            with zipfile.ZipFile(filename, 'w', compression) as file:
                if dump_schema:
                    self.write_initial_setup(file)
                if dump_data:
                    self.add_related_data(full_tables, partial_tables)
                    self.write_full_tables(file, full_tables)
                    self.write_partial_tables(file, partial_tables)

    def input_check(self, full_tables, partial_tables):
        if full_tables and partial_tables:
            common_tables = set(full_tables) & set(partial_tables)
            if common_tables:
                raise ValueError(
                    '`partial_tables` should not contain tables from `full_tables`. Common tables: {tables}'.format(
                        tables=', '.join(common_tables)
                    )
                )

    def add_related_data(self, full_tables, partial_tables):
        """
        Updates selects for partial tables to grab all objects, that are referenced by full / partial tables.
        """
        tables = self.get_tables_for_related_data(full_tables, partial_tables)
        for table in tables:
            self.update_partial_tables(table, full_tables, partial_tables)

    def get_tables_for_related_data(self, full_tables, partial_tables):
        return tuple(full_tables) + tuple(partial_tables.keys())

    def update_partial_tables(self, table, full_tables, partial_tables):
        self.update_recursive_relations(table, full_tables, partial_tables)
        self.update_non_recursive_relations(table, full_tables, partial_tables)

    def update_recursive_relations(self, table, full_tables, partial_tables):
        for foreign_key in self.get_foreign_keys(table, full_tables, recursive=True):
            if table in partial_tables:
                partial_tables[table] = RECURSIVE_QUERY_TEMPLATE.format(
                    source=partial_tables[table], target=table, **foreign_key
                )

    def update_non_recursive_relations(self, table, full_tables, partial_tables):
        for foreign_key in self.get_foreign_keys(table, full_tables):
            sql = self.get_related_data_sql(foreign_key, full_tables, partial_tables)
            if sql:
                foreign_table = foreign_key['foreign_table_name']
                if foreign_table in partial_tables:
                    partial_tables[foreign_table] += ' UNION ' + sql
                else:
                    partial_tables[foreign_table] = sql
                # Now we select more than before for given table, so we have to do check related data for it.
                self.update_partial_tables(foreign_table, full_tables, partial_tables)

    def get_foreign_keys(self, table, full_tables=(), recursive=False):
        """
        Looks for foreign keys in the given table. Excluding ones, that will be dumped in ``full_tables``.
        """
        raise NotImplementedError

    def get_related_data_sql(self, foreign_key, full_tables, partial_tables):
        """
        Generates SQL to select related data, that is referred from another table.
        """
        table_name = foreign_key['table_name']
        if table_name in full_tables:
            source = foreign_key['table_name']
        elif table_name in partial_tables:
            source = '({}) T'.format(partial_tables[table_name])
        else:
            return
        return '''
            SELECT
                *
            FROM {foreign_table_name}
            WHERE {foreign_column_name} IN (
                SELECT {column_name} FROM {source}
            )'''.format(source=source, **foreign_key)

    def write_initial_setup(self, file):
        self.write_schema(file)

    def write_schema(self, file):
        """
        Writes a DB schema, functions, etc to the archive.
        """
        schema = self.dump_schema()
        file.writestr(self.schema_filename, schema)

    def dump_schema(self):
        raise NotImplementedError

    def write_full_tables(self, file, tables):
        """
        Writes a complete tables dump to the archive.
        """
        for table_name in tables:
            self.write_data_file(file, table_name, 'SELECT * FROM {0}'.format(table_name))

    def write_partial_tables(self, file, config):
        for table_name, sql in config.items():
            self.write_data_file(file, table_name, sql)

    def write_data_file(self, file, table_name, sql):
        data = self.export_to_csv(sql)
        file.writestr('{0}{1}.csv'.format(self.data_dir, table_name), data)

    def export_to_csv(self, sql):
        raise NotImplementedError

    # Database re-creation

    def recreate_database(self, owner=None):
        """
        Drops all connections to the database, drops the database and creates it again.
        """
        self.drop_database(self.dbname)
        self.create_database(self.dbname, owner)
        self.cache_clear()

    def drop_database(self, dbname):
        raise NotImplementedError

    def create_database(self, dbname, *args, **kwargs):
        raise NotImplementedError

    def truncate(self):
        """Truncates all tables in the DB. Alternative for the re-creation option"""
        raise NotImplementedError

    # Loading the dump

    def load(self, filename):
        """
        Loads schema, sequences and data into the database.
        """
        with self.log_time('Total execution time: %s'):
            archive = zipfile.ZipFile(filename)
            self.initial_setup(archive)
            self.load_data(archive)

    def initial_setup(self, archive):
        """
        Loads schema and initial database configuration.
        """
        name_list = archive.namelist()
        for filename in self.initial_setup_files:
            if filename not in name_list:
                continue
            sql = archive.read(filename)
            self.run_setup_file(sql)

    def run_setup_file(self, sql):
        return self.run(sql)

    def load_data(self, archive):
        """
        Loads all data from data files inside the archive to the database.
        """
        with self.transaction():
            for name in archive.namelist():
                if name.startswith(self.data_dir):
                    fd = archive.open(name)
                    table_name = os.path.basename(name).split('.')[0]
                    self.load_data_file(table_name, fd)

    def load_data_file(self, table_name, fd):
        """
        Loads a data file into the database.
        """
        raise NotImplementedError


RECURSIVE_QUERY_TEMPLATE = '''
WITH RECURSIVE recursive_cte AS (
  SELECT * FROM ({source}) S
  UNION
  SELECT T.*
  FROM {table_name} T
  INNER JOIN recursive_cte ON (recursive_cte.{column_name} = T.{foreign_column_name})
)
SELECT * FROM recursive_cte
'''

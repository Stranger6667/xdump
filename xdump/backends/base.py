# coding: utf-8
import zipfile
from contextlib import contextmanager
from functools import lru_cache

import attr


@attr.s(cmp=False)
class BaseBackend:
    dbname = attr.ib()
    user = attr.ib()
    password = attr.ib()
    host = attr.ib()
    port = attr.ib()
    connections = {'default': {}}
    data_dir = 'dump/data/'

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

    # Low-level commands executors

    def run_dump(self, *args, **kwargs):
        """
        Runs built-in utility for data / schema dumping.
        """
        raise NotImplementedError

    def run(self, sql, params=None, using='default'):
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

    # Dump & load

    def dump(self, filename, full_tables=(), partial_tables=None):
        partial_tables = partial_tables or {}
        with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as file:
            self.write_initial_setup(file)
            self.write_full_tables(file, full_tables)
            self.write_partial_tables(file, partial_tables)

    def write_full_tables(self, file, tables):
        """
        Writes a complete tables dump to the archive.
        """
        for table_name in tables:
            self.write_data_file(file, table_name, f'SELECT * FROM {table_name}')

    def write_partial_tables(self, file, config):
        for table_name, sql in config.items():
            self.write_data_file(file, table_name, sql)

    def write_data_file(self, file, table_name, sql):
        raise NotImplementedError

    def populate_database(self, filename):
        self.recreate_database()
        self.load(filename)

    def recreate_database(self):
        """
        Drops all connections to the database, drops the database and creates it again.
        """
        self.drop_connections(self.dbname)
        self.drop_database(self.dbname)
        self.create_database(self.dbname, self.user)
        self.get_cursor.cache_clear()
        self.get_connection.cache_clear()

    def load(self, filename):
        """
        Loads schema, sequences and data into the database.
        """
        archive = zipfile.ZipFile(filename)
        self.initial_setup(archive)
        self.load_data(archive)

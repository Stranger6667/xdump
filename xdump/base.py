# coding: utf-8
import zipfile
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

import attr


@attr.s(cmp=False)
class BaseBackend:
    dbname = attr.ib()
    user = attr.ib()
    password = attr.ib()
    host = attr.ib()
    port = attr.ib(convert=str)
    connections = {'default': {}}
    schema_filename = 'dump/schema.sql'
    initial_setup_files = (schema_filename, )
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

    # Dumping the data

    def dump(self, filename, full_tables=(), partial_tables=None):
        """
        Creates a dump, which could be used to restore the database.
        """
        partial_tables = partial_tables or {}
        with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as file:
            self.write_initial_setup(file)
            self.write_full_tables(file, full_tables)
            self.write_partial_tables(file, partial_tables)

    def write_initial_setup(self, file):
        self.write_schema(file)

    def write_schema(self, file):
        """
        Writes a DB schema, functions, etc to the archive.
        """
        schema = self.dump_schema()
        file.writestr(self.schema_filename, schema)

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
        data = self.export_to_csv(sql)
        file.writestr(f'{self.data_dir}{table_name}.csv', data)

    def export_to_csv(self, sql):
        raise NotImplementedError

    # Database re-creation

    def recreate_database(self, owner=None):
        """
        Drops all connections to the database, drops the database and creates it again.
        """
        if owner is None:
            owner = self.user
        self.drop_database(self.dbname)
        self.create_database(self.dbname, owner)
        self.get_cursor.cache_clear()
        self.get_connection.cache_clear()

    def drop_database(self, dbname):
        raise NotImplementedError

    def create_database(self, dbname, *args, **kwargs):
        raise NotImplementedError

    # Loading the dump

    def load(self, filename):
        """
        Loads schema, sequences and data into the database.
        """
        archive = zipfile.ZipFile(filename)
        self.initial_setup(archive)
        self.load_data(archive)

    def initial_setup(self, archive):
        """
        Loads schema and initial database configuration.
        """
        for filename in self.initial_setup_files:
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
                    table_name = Path(name).stem
                    self.load_data_file(table_name, fd)

    def load_data_file(self, table_name, fd):
        """
        Loads a data file into the database.
        """
        raise NotImplementedError

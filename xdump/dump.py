# coding: utf-8
import os
import subprocess
import zipfile
from io import BytesIO
from pathlib import Path

import attr
import psycopg2
from cached_property import cached_property
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_REPEATABLE_READ
from psycopg2.extras import RealDictConnection

from .utils import make_options


SELECTABLE_TABLES_SQL = '''
SELECT table_name
FROM information_schema.tables
WHERE
    table_schema NOT IN ('pg_catalog', 'information_schema') AND
    table_schema NOT LIKE 'pg_toast%'
'''
SEQUENCES_SQL = '''
SELECT relname FROM pg_class WHERE relkind = 'S'
'''

SCHEMA_FILENAME = 'dump/schema.sql'
SEQUENCES_FILENAME = 'dump/sequences.sql'
DATA_DIR = 'dump/data/'


def auto_reconnect(func):

    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except psycopg2.OperationalError as exc:
            if str(exc).startswith('terminating connection due to administrator command'):
                del self.connection
                del self.cursor
                return func(self, *args, **kwargs)
            raise

    return wrapper


@attr.s
class Dumper:
    dbname = attr.ib()
    user = attr.ib()
    password = attr.ib(default=None)
    host = attr.ib(default='127.0.0.1')
    port = attr.ib(default=5432)

    @cached_property
    def connection(self):
        return self.connect(ISOLATION_LEVEL_REPEATABLE_READ)

    @cached_property
    def maintenance_connection(self):
        return self.connect(ISOLATION_LEVEL_AUTOCOMMIT, dbname='postgres')

    @cached_property
    def cursor(self):
        return self.connection.cursor()

    @cached_property
    def maintenance_cursor(self):
        return self.maintenance_connection.cursor()

    def get_cursor(self, alias='default'):
        if alias == 'default':
            return self.cursor
        elif alias == 'maintenance':
            return self.maintenance_cursor
        raise ValueError

    def connect(self, isolation_level, **kwargs):
        for option in ('dbname', 'user', 'password', 'host', 'port'):
            kwargs.setdefault(option, getattr(self, option))
        connection = psycopg2.connect(connection_factory=RealDictConnection, **kwargs)
        connection.set_isolation_level(isolation_level)
        return connection

    @auto_reconnect
    def run(self, sql, params=None, using='default'):
        cursor = self.get_cursor(using)
        cursor.execute(sql, params)
        try:
            return cursor.fetchall()
        except psycopg2.ProgrammingError:
            pass

    def export_to_csv(self, sql):
        """
        Exports the result of the given sql to CSV with a help of COPY statement.
        """
        with BytesIO() as output:
            self.cursor.copy_expert(f'COPY ({sql}) TO STDOUT WITH CSV HEADER', output)
            return output.getvalue()

    @property
    def pg_dump_environment(self):
        if self.password:
            return {**os.environ, 'PGPASSWORD': self.password}
        return os.environ.copy()

    def pg_dump(self, *args):
        process = subprocess.Popen(
            [
                'pg_dump',
                '-U', self.user,
                '-h', self.host,
                '-p', self.port,
                '-d', self.dbname,
                *args,
            ],
            stdout=subprocess.PIPE,
            env=self.pg_dump_environment
        )
        return process.communicate()[0]

    def dump(self, filename, full_tables=None, partial_tables=None):
        full_tables = full_tables or ()
        partial_tables = partial_tables or {}
        with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as file:
            self.write_schema(file)
            self.write_sequences(file)
            self.write_full_tables(file, full_tables)
            self.write_partial_tables(file, partial_tables)

    def write_schema(self, file):
        """
        Writes a DB schema, functions, etc to the archive.
        """
        schema = self.dump_schema()
        file.writestr(SCHEMA_FILENAME, schema)

    def get_selectable_tables(self):
        """
        Returns a list of tables, from which current user can select data.
        """
        return [row['table_name'] for row in self.run(SELECTABLE_TABLES_SQL)]

    def dump_schema(self):
        """
        Produces SQL for the schema of the database.
        """
        selectable_tables = self.get_selectable_tables()
        return self.pg_dump(
            '-s',  # Schema-only
            '-x',  # Do not dump privileges
            *make_options('-t', selectable_tables)
        )

    def get_sequences(self):
        """
        To be able to modify our loaded dump we need to load exact sequences states.
        """
        return [row['relname'] for row in self.run(SEQUENCES_SQL)]

    def dump_sequences(self):
        sequences = self.get_sequences()
        return self.pg_dump(
            '-a',  # Data-only
            *make_options('-t', sequences)
        )

    def write_sequences(self, file):
        sequences = self.dump_sequences()
        file.writestr(SEQUENCES_FILENAME, sequences)

    def write_csv(self, file, table_name, sql):
        data = self.export_to_csv(sql)
        file.writestr(f'{DATA_DIR}{table_name}.csv', data)

    def write_full_tables(self, file, tables):
        """
        Writes a complete tables dump in CSV format to the archive.
        """
        for table_name in tables:
            self.write_csv(file, table_name, f'SELECT * FROM {table_name}')

    def write_partial_tables(self, file, config):
        for table_name, sql in config.items():
            self.write_csv(file, table_name, sql)

    def recreate_database(self, dbname, owner):
        self.drop_connections(dbname)
        self.drop_database(dbname)
        self.create_database(dbname, owner)

    def drop_connections(self, dbname):
        self.run('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s', [dbname], 'maintenance')

    def drop_database(self, dbname):
        self.run(f'DROP DATABASE IF EXISTS {dbname}', using='maintenance')

    def create_database(self, dbname, owner):
        self.run(f"CREATE DATABASE {dbname} WITH OWNER {owner}", using='maintenance')

    def load(self, filename):
        archive = zipfile.ZipFile(filename)
        self.initial_setup(archive)
        self.load_data(archive)

    def run_archive_file(self, archive, filename):
        sql = archive.read(filename)
        self.run(sql)

    def initial_setup(self, archive):
        self.load_schema(archive)
        self.load_sequences(archive)

    def load_schema(self, archive):
        self.run_archive_file(archive, SCHEMA_FILENAME)

    def load_sequences(self, archive):
        self.run_archive_file(archive, SEQUENCES_FILENAME)

    def load_data(self, archive):
        self.run('BEGIN')
        for name in archive.namelist():
            if name.startswith(DATA_DIR):
                fp = archive.open(name)
                filename = Path(name).stem
                self.cursor.copy_expert(f'COPY {filename} FROM STDIN WITH CSV HEADER', fp)
        self.run('COMMIT')

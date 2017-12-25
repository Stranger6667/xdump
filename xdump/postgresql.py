# coding: utf-8
import os
import subprocess
from io import BytesIO
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_REPEATABLE_READ
from psycopg2.extras import RealDictConnection

from xdump.utils import make_options
from .base import BaseBackend


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


class Backend(BaseBackend):
    connections = {
        'default': {
            'isolation_level': ISOLATION_LEVEL_REPEATABLE_READ,
        },
        'maintenance': {
            'dbname': 'postgres',
            'isolation_level': ISOLATION_LEVEL_AUTOCOMMIT,
        }
    }

    def connect(self, isolation_level, **kwargs):
        kwargs = self.get_connection_kwargs(**kwargs)
        connection = psycopg2.connect(**kwargs)
        connection.set_isolation_level(isolation_level)
        return connection

    def get_connection_kwargs(self, **kwargs):
        return super().get_connection_kwargs(connection_factory=RealDictConnection, **kwargs)

    def handle_run_exception(self, exc):
        """
        Suppress exception when there is nothing to fetch.
        """
        if str(exc) != 'no results to fetch':
            raise exc

    @property
    def pg_dump_environment(self):
        if self.password:
            return {**os.environ, 'PGPASSWORD': self.password}
        return os.environ.copy()

    def run_dump(self, *args, **kwargs):
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

    def write_initial_setup(self, file):
        self.write_schema(file)
        self.write_sequences(file)

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
        return self.run_dump(
            '-s',  # Schema-only
            '-x',  # Do not dump privileges
            *make_options('-t', selectable_tables)
        )

    def write_schema(self, file):
        """
        Writes a DB schema, functions, etc to the archive.
        """
        schema = self.dump_schema()
        file.writestr(SCHEMA_FILENAME, schema)

    def get_sequences(self):
        """
        To be able to modify our loaded dump we need to load exact sequences states.
        """
        return [row['relname'] for row in self.run(SEQUENCES_SQL)]

    def dump_sequences(self):
        sequences = self.get_sequences()
        return self.run_dump(
            '-a',  # Data-only
            *make_options('-t', sequences)
        )

    def write_sequences(self, file):
        sequences = self.dump_sequences()
        file.writestr(SEQUENCES_FILENAME, sequences)

    def copy_expert(self, *args, **kwargs):
        cursor = self.get_cursor()
        return cursor.copy_expert(*args, **kwargs)

    def export_to_csv(self, sql):
        """
        Exports the result of the given sql to CSV with a help of COPY statement.
        """
        with BytesIO() as output:
            self.copy_expert(f'COPY ({sql}) TO STDOUT WITH CSV HEADER', output)
            return output.getvalue()

    def write_data_file(self, file, table_name, sql):
        data = self.export_to_csv(sql)
        file.writestr(f'{self.data_dir}{table_name}.csv', data)

    def drop_connections(self, dbname):
        self.run('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s', [dbname], 'maintenance')

    def drop_database(self, dbname):
        self.run(f'DROP DATABASE IF EXISTS {dbname}', using='maintenance')

    def create_database(self, dbname, owner):
        self.run(f"CREATE DATABASE {dbname} WITH OWNER {owner}", using='maintenance')

    def initial_setup(self, archive):
        """
        Loads schema and sequences SQL.
        """
        for filename in (SCHEMA_FILENAME, SEQUENCES_FILENAME):
            sql = archive.read(filename)
            self.run(sql)

    def load_data(self, archive):
        """
        Loads all data from CSV files inside the archive to the database.
        """
        with self.transaction():
            for name in archive.namelist():
                if name.startswith(self.data_dir):
                    fp = archive.open(name)
                    filename = Path(name).stem
                    self.copy_expert(f'COPY {filename} FROM STDIN WITH CSV HEADER', fp)

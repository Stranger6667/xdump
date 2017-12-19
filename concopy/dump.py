# coding: utf-8
import os
import subprocess
import zipfile
from io import BytesIO

import attr
import psycopg2
from cached_property import cached_property
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
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


class Dump(zipfile.ZipFile):

    def write_data(self, table_name, data):
        self.writestr(f'dump/data/{table_name}.csv', data)


@attr.s
class Dumper:
    dbname = attr.ib()
    user = attr.ib()
    password = attr.ib(default=None)
    host = attr.ib(default='127.0.0.1')
    port = attr.ib(default=5432)

    @cached_property
    def connection(self):
        connection = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            connection_factory=RealDictConnection
        )
        connection.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
        return connection

    @cached_property
    def cursor(self):
        return self.connection.cursor()

    def run(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

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
        with Dump(filename, 'w', zipfile.ZIP_DEFLATED) as file:
            self.write_schema(file)
            self.write_sequences(file)
            self.write_full_tables(file, full_tables)
            self.write_partial_tables(file, partial_tables)

    def write_schema(self, file):
        """
        Writes a DB schema, functions, etc to the archive.
        """
        schema = self.dump_schema()
        file.writestr('dump/schema.sql', schema)

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
        file.writestr('dump/sequences.sql', sequences)

    def write_csv(self, file, table_name, sql):
        data = self.export_to_csv(sql)
        file.write_data(table_name, data)

    def write_full_tables(self, file, tables):
        """
        Writes a complete tables dump in CSV format to the archive.
        """
        for table_name in tables:
            self.write_csv(file, table_name, f'SELECT * FROM {table_name}')

    def write_partial_tables(self, file, config):
        for table_name, sql in config.items():
            self.write_csv(file, table_name, sql)

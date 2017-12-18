# coding: utf-8
import os
import subprocess
import zipfile
from io import BytesIO

import psycopg2

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
FULL_TABLES = ()


class Dump(zipfile.ZipFile):

    def write_data(self, table_name, data):
        self.writestr(f'dump/data/{table_name}.csv', data)


class Dumper:

    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    @property
    def connection(self):
        if not hasattr(self, '_connection'):
            self._connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                connection_factory=psycopg2.extras.RealDictConnection
            )
        return self._connection

    @property
    def cursor(self):
        if not hasattr(self, '_cursor'):
            self._cursor = self.connection.cursor()
        return self._cursor

    def run(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def copy_to_stdout(self, sql):
        with BytesIO() as output:
            self.cursor.copy_expert(f'COPY ({sql}) TO STDOUT WITH CSV HEADER', output)
            return output.getvalue()

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
            env={**os.environ, 'PGPASSWORD': self.password},
        )
        return process.communicate()[0]

    def dump(self, filename, full_tables):
        with Dump(filename, 'w', zipfile.ZIP_DEFLATED) as file:
            self.write_schema(file)
            self.write_sequences(file)
            self.write_full_tables(file)

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
            *make_options(sequences, '-t')
        )

    def write_sequences(self, file):
        sequences = self.dump_sequences()
        file.writestr('dump/sequences.sql', sequences)

    def write_full_tables(self, file):
        """
        Writes a complete tables dump in CSV format to the archive.
        """
        for table_name in FULL_TABLES:
            data = self.copy_to_stdout(f'SELECT * FROM {table_name}')
            file.write_data(table_name, data)

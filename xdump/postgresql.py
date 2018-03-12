# coding: utf-8
import os
import subprocess
from io import BytesIO

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_REPEATABLE_READ
from psycopg2.extras import RealDictConnection

from .base import BaseBackend
from .utils import make_options


SEQUENCES_SQL = "SELECT relname FROM pg_class WHERE relkind = 'S'"
BASE_RELATIONS_QUERY = '''
SELECT
    tc.constraint_name, tc.table_name, kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
WHERE
    constraint_type = 'FOREIGN KEY' AND
    tc.table_name {operator} ccu.table_name AND
    tc.table_name = %(table_name)s AND
    NOT(ccu.table_name = ANY(%(full_tables)s))
'''


class PostgreSQLBackend(BaseBackend):
    sequences_filename = 'dump/sequences.sql'
    initial_setup_files = BaseBackend.initial_setup_files + (sequences_filename, )
    connections = {
        'default': {
            'isolation_level': ISOLATION_LEVEL_REPEATABLE_READ,
        },
        'maintenance': {
            'dbname': 'postgres',
            'isolation_level': ISOLATION_LEVEL_AUTOCOMMIT,
        }
    }
    tables_sql = '''
    SELECT table_name
    FROM information_schema.tables
    WHERE
        table_schema NOT IN ('pg_catalog', 'information_schema') AND
        table_schema NOT LIKE 'pg_toast%'
    '''
    non_recursive_relations_query = BASE_RELATIONS_QUERY.format(operator='!=')
    recursive_relations_query = BASE_RELATIONS_QUERY.format(operator='=')

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
    def run_dump_environment(self):
        environ = os.environ.copy()
        if self.password:
            environ['PGPASSWORD'] = self.password
        return environ

    def run_dump(self, *args, **kwargs):
        process = subprocess.Popen(
            (
                'pg_dump',
                '-U', self.user,
                '-h', self.host,
                '-p', self.port,
                '-d', self.dbname,
            ) + args,
            stdout=subprocess.PIPE,
            env=self.run_dump_environment
        )
        return process.communicate()[0]

    def write_initial_setup(self, file):
        super().write_initial_setup(file)
        self.write_sequences(file)

    def dump_schema(self):
        """
        Produces SQL for the schema of the database.
        """
        return self.run_dump(
            '-s',  # Schema-only
            '-x',  # Do not dump privileges
        )

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
        file.writestr(self.sequences_filename, sequences)

    def copy_expert(self, *args, **kwargs):
        cursor = self.get_cursor()
        return cursor.copy_expert(*args, **kwargs)

    def export_to_csv(self, sql):
        """
        Exports the result of the given sql to CSV with a help of COPY statement.
        """
        with BytesIO() as output:
            self.copy_expert('COPY ({0}) TO STDOUT WITH CSV HEADER'.format(sql), output)
            return output.getvalue()

    def recreate_database(self, owner=None):
        self.drop_connections(self.dbname)
        super().recreate_database(owner)

    def drop_connections(self, dbname):
        self.run('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s', [dbname], 'maintenance')

    def drop_database(self, dbname):
        self.run('DROP DATABASE IF EXISTS {0}'.format(dbname), using='maintenance')

    def create_database(self, dbname, owner):
        self.run('CREATE DATABASE {0} WITH OWNER {1}'.format(dbname, owner), using='maintenance')

    def load_data_file(self, table_name, fd):
        self.copy_expert('COPY {0} FROM STDIN WITH CSV HEADER'.format(table_name), fd)

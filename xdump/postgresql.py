# coding: utf-8
import os
import subprocess
from io import BytesIO

import attr
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_REPEATABLE_READ
from psycopg2.extras import RealDictConnection

from .base import BaseBackend
from .utils import make_options


TABLES_SQL = "SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace"
SEQUENCES_SQL = "SELECT relname FROM pg_class WHERE relkind = 'S'"
# The query below doesn't use `information_schema.table_constraints` and ``, but instead uses its modified versions
# to mitigate permissions insufficiency on that views (they filter data by permissions of the current user)
# Subqueries for constraints other than FOREIGN KEY are removed as well.
BASE_RELATIONS_QUERY = '''
SELECT
  DISTINCT
  TC.constraint_name,
  TC.table_name,
  KCU.column_name,
  CCU.foreign_table_name,
  CCU.foreign_column_name
FROM
  (
    SELECT
      CN.conname AS constraint_name,
      CL.relname AS table_name
    FROM pg_namespace NS,
      pg_constraint CN,
      pg_class CL
    WHERE
      NS.oid = CL.relnamespace AND
      CN.conrelid = CL.oid AND
      CN.contype = 'f' AND
      CL.relkind = 'r' AND
      NOT pg_is_other_temp_schema(NS.oid)
    ) AS TC
    JOIN information_schema.key_column_usage AS KCU
      ON TC.constraint_name = KCU.constraint_name AND
         KCU.table_name = TC.table_name
    JOIN (
      SELECT
        CL.relname AS foreign_table_name,
        AT.attname AS foreign_column_name,
        CN.conname AS constraint_name
      FROM pg_class CL,
        pg_attribute AT,
        pg_constraint CN
      WHERE
        CL.oid = AT.attrelid AND
        CL.oid = CN.confrelid AND
        AT.attnum = ANY (CN.confkey) AND
        NOT AT.attisdropped AND
        CN.contype = 'f' AND
        CL.relkind = 'r'
      ) AS CCU
    ON CCU.constraint_name = TC.constraint_name
'''


@attr.s(cmp=False)
class PostgreSQLBackend(BaseBackend):
    dbname = attr.ib()
    user = attr.ib()
    password = attr.ib()
    host = attr.ib()
    port = attr.ib(convert=str)
    verbosity = attr.ib(convert=int, default=0)
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

    def connect(self, isolation_level, **kwargs):
        kwargs = self.get_connection_kwargs(**kwargs)
        connection = psycopg2.connect(**kwargs)
        connection.set_isolation_level(isolation_level)
        return connection

    def get_connection_kwargs(self, **kwargs):
        return super(PostgreSQLBackend, self).get_connection_kwargs(connection_factory=RealDictConnection, **kwargs)

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
        super(PostgreSQLBackend, self).write_initial_setup(file)
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

    def add_related_data(self, full_tables, partial_tables):
        if full_tables:
            query = BASE_RELATIONS_QUERY + ' WHERE NOT(CCU.foreign_table_name = ANY(%(full_tables)s))'
            kwargs = {'full_tables': list(full_tables)}
        else:
            query = BASE_RELATIONS_QUERY
            kwargs = {}
        self._related_data = self.run(query, kwargs)
        super(PostgreSQLBackend, self).add_related_data(full_tables, partial_tables)

    def get_foreign_keys(self, table, full_tables=(), recursive=False):
        # NOTE, `full_tables` is not used, because it is filtered in `BASE_RELATIONS_QUERY`
        for foreign_key in self._related_data:
            if foreign_key['table_name'] == table:
                if foreign_key['foreign_table_name'] == table and not recursive:
                    continue
                if foreign_key['foreign_table_name'] != table and recursive:
                    continue
                yield foreign_key

    def copy_expert(self, sql, file, **kwargs):
        with self.log_query(sql):
            cursor = self.get_cursor()
            return cursor.copy_expert(sql, file, **kwargs)

    def export_to_csv(self, sql):
        """
        Exports the result of the given sql to CSV with a help of COPY statement.
        """
        with BytesIO() as output:
            self.copy_expert('COPY ({0}) TO STDOUT WITH CSV HEADER'.format(sql), output)
            return output.getvalue()

    def get_search_path(self):
        return self.run('show search_path;')[0]['search_path']

    def restore_search_path(self, search_path):
        self.run("SELECT pg_catalog.set_config('search_path', '{0}', false);".format(search_path))

    def initial_setup(self, archive):
        search_path = self.get_search_path()
        super(PostgreSQLBackend, self).initial_setup(archive)
        self.restore_search_path(search_path)

    def recreate_database(self, owner=None):
        if owner is None:
            owner = self.user
        self.drop_connections(self.dbname)
        super(PostgreSQLBackend, self).recreate_database(owner)

    def drop_connections(self, dbname):
        self.run('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s', [dbname], 'maintenance')

    def drop_database(self, dbname):
        self.run('DROP DATABASE IF EXISTS {0}'.format(dbname), using='maintenance')

    def create_database(self, dbname, owner):
        self.run('CREATE DATABASE {0} WITH OWNER {1}'.format(dbname, owner), using='maintenance')

    def truncate(self):
        tables = [row['relname'] for row in self.run(TABLES_SQL)]
        self.run('TRUNCATE TABLE {0} RESTART IDENTITY CASCADE'.format(', '.join(tables)))

    def load_data_file(self, table_name, fd):
        self.copy_expert('COPY {0} FROM STDIN WITH CSV HEADER'.format(table_name), fd)

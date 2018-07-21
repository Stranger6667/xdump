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
# The query below doesn't use `information_schema.table_constraints` and ``, but instead uses its modified versions
# to mitigate permissions insufficiency on that views (they filter data by permissions of the current user)
BASE_RELATIONS_QUERY = '''
SELECT
    DISTINCT
    tc.constraint_name, tc.table_name, kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM
    (
    SELECT
        c.conname::information_schema.sql_identifier AS constraint_name,
        r.relname::information_schema.sql_identifier AS table_name,
        CASE c.contype
            WHEN 'c'::"char" THEN 'CHECK'::text
            WHEN 'f'::"char" THEN 'FOREIGN KEY'::text
            WHEN 'p'::"char" THEN 'PRIMARY KEY'::text
            WHEN 'u'::"char" THEN 'UNIQUE'::text
            ELSE NULL::text
        END::information_schema.character_data AS constraint_type
   FROM pg_namespace nc,
    pg_namespace nr,
    pg_constraint c,
    pg_class r
  WHERE
    nc.oid = c.connamespace AND
    nr.oid = r.relnamespace AND
    c.conrelid = r.oid AND
    (c.contype <> ALL (ARRAY['t'::"char", 'x'::"char"])) AND
    r.relkind = 'r'::"char" AND NOT pg_is_other_temp_schema(nr.oid)
UNION ALL
 SELECT
    (
      ((((nr.oid::text || '_'::text) || r.oid::text) || '_'::text) || a.attnum::text) || '_not_null'::text
    )::information_schema.sql_identifier AS constraint_name,
    r.relname::information_schema.sql_identifier AS table_name,
    'CHECK'::character varying::information_schema.character_data AS constraint_type
   FROM pg_namespace nr,
    pg_class r,
    pg_attribute a
  WHERE
    nr.oid = r.relnamespace AND
    r.oid = a.attrelid AND
    a.attnotnull AND
    a.attnum > 0 AND
    NOT a.attisdropped AND
    r.relkind = 'r'::"char" AND
    NOT pg_is_other_temp_schema(nr.oid)
    ) AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name AND kcu.table_name = tc.table_name
    JOIN (
    SELECT
    x.tblname::information_schema.sql_identifier AS table_name,
    x.colname::information_schema.sql_identifier AS column_name,
    x.cstrname::information_schema.sql_identifier AS constraint_name
   FROM ( SELECT DISTINCT
            r.relname,
            a.attname,
            c.conname
           FROM pg_namespace nr,
            pg_class r,
            pg_attribute a,
            pg_depend d,
            pg_namespace nc,
            pg_constraint c
          WHERE
            nr.oid = r.relnamespace AND
            r.oid = a.attrelid AND
            d.refclassid = 'pg_class'::regclass::oid AND
            d.refobjid = r.oid AND
            d.refobjsubid = a.attnum AND
            d.classid = 'pg_constraint'::regclass::oid AND
            d.objid = c.oid AND
            c.connamespace = nc.oid AND
            c.contype = 'c'::"char" AND
            r.relkind = 'r'::"char" AND
            NOT a.attisdropped
        UNION ALL
         SELECT
            r.relname,
            a.attname,
            c.conname
           FROM pg_namespace nr,
            pg_class r,
            pg_attribute a,
            pg_namespace nc,
            pg_constraint c
          WHERE
            nr.oid = r.relnamespace AND
            r.oid = a.attrelid AND
            nc.oid = c.connamespace AND
            CASE
              WHEN c.contype = 'f'::"char"
                THEN
                  r.oid = c.confrelid AND (a.attnum = ANY (c.confkey))
                ELSE
                  r.oid = c.conrelid AND (a.attnum = ANY (c.conkey))
            END AND
            NOT a.attisdropped AND
            (
              c.contype = ANY (ARRAY['p'::"char", 'u'::"char", 'f'::"char"])) AND
              r.relkind = 'r'::"char"
            ) x(tblname, colname, cstrname)
    ) AS ccu
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
        super().initial_setup(archive)
        self.restore_search_path(search_path)

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

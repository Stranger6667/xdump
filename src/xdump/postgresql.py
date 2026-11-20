import os
import re
import subprocess
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from io import BytesIO
from typing import IO, Any

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_REPEATABLE_READ
from psycopg2.extras import RealDictConnection

from .base import BaseBackend, ForeignKey
from .utils import make_options

TABLES_SQL = "SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace"
SEQUENCES_SQL = "SELECT relname FROM pg_class WHERE relkind = 'S'"
# `pg_dump` from PostgreSQL 13.22 / 14.19 / 15.14 / 16.10 / 17.6 wraps its output in `\restrict` / `\unrestrict` psql
# meta-commands (CVE-2025-8714). They are not SQL, and the server rejects them.
PSQL_RESTRICT_RE = re.compile(rb"^\\(un)?restrict .*\n?", re.MULTILINE)
# The query below doesn't use `information_schema.table_constraints` and ``, but instead uses its modified versions
# to mitigate permissions insufficiency on that views (they filter data by permissions of the current user)
# Subqueries for constraints other than FOREIGN KEY are removed as well.
BASE_RELATIONS_QUERY = """
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
"""


@dataclass(eq=False)
class PostgreSQLBackend(BaseBackend):
    dbname: str
    user: str
    password: str | None
    host: str
    port: str
    verbosity: int = 0
    sequences_filename = "dump/sequences.sql"
    initial_setup_files = BaseBackend.initial_setup_files + (sequences_filename,)
    connections = {
        "default": {
            "isolation_level": ISOLATION_LEVEL_REPEATABLE_READ,
        },
        "maintenance": {
            "dbname": "postgres",
            "isolation_level": ISOLATION_LEVEL_AUTOCOMMIT,
        },
    }

    def __post_init__(self) -> None:
        self.port = str(self.port)
        self.verbosity = int(self.verbosity)

    def connect(self, **kwargs: Any) -> Any:
        isolation_level = kwargs.pop("isolation_level")
        connection = psycopg2.connect(**self.get_connection_kwargs(**kwargs))
        connection.set_isolation_level(isolation_level)
        return connection

    def get_connection_kwargs(self, **kwargs: Any) -> dict[str, Any]:
        return super().get_connection_kwargs(connection_factory=RealDictConnection, **kwargs)

    def handle_run_exception(self, exc: Exception) -> None:
        """Suppress exception when there is nothing to fetch."""
        if str(exc) != "no results to fetch":
            raise exc

    @property
    def run_dump_environment(self) -> dict[str, str]:
        environ = os.environ.copy()
        if self.password:
            environ["PGPASSWORD"] = self.password
        return environ

    def run_dump(self, *args: str) -> bytes:
        process = subprocess.Popen(
            (
                "pg_dump",
                "-U",
                self.user,
                "-h",
                self.host,
                "-p",
                self.port,
                "-d",
                self.dbname,
            )
            + args,
            stdout=subprocess.PIPE,
            env=self.run_dump_environment,
        )
        return process.communicate()[0]

    def write_initial_setup(self, file: zipfile.ZipFile) -> None:
        super().write_initial_setup(file)
        self.write_sequences(file)

    def dump_schema(self) -> bytes:
        """Produces SQL for the schema of the database."""
        return self.run_dump(
            "-s",  # Schema-only
            "-x",  # Do not dump privileges
        )

    def get_sequences(self) -> list[str]:
        """To be able to modify our loaded dump we need to load exact sequences states."""
        return [row["relname"] for row in self.run(SEQUENCES_SQL)]

    def dump_sequences(self) -> bytes:
        sequences = self.get_sequences()
        return self.run_dump("-a", *make_options("-t", sequences))  # Data-only

    def write_sequences(self, file: zipfile.ZipFile) -> None:
        sequences = self.dump_sequences()
        file.writestr(self.sequences_filename, sequences)

    def add_related_data(self, full_tables: Sequence[str], partial_tables: dict[str, str]) -> None:
        if full_tables:
            query = BASE_RELATIONS_QUERY + " WHERE NOT(CCU.foreign_table_name = ANY(%(full_tables)s))"
            kwargs: dict[str, Any] = {"full_tables": list(full_tables)}
        else:
            query = BASE_RELATIONS_QUERY
            kwargs = {}
        self._related_data: list[ForeignKey] = self.run(query, kwargs)
        super().add_related_data(full_tables, partial_tables)

    def get_foreign_keys(
        self, table: str, full_tables: Sequence[str] = (), recursive: bool = False
    ) -> Iterator[ForeignKey]:
        # NOTE, `full_tables` is not used, because it is filtered in `BASE_RELATIONS_QUERY`
        for foreign_key in self._related_data:
            if foreign_key["table_name"] == table:
                if foreign_key["foreign_table_name"] == table and not recursive:
                    continue
                if foreign_key["foreign_table_name"] != table and recursive:
                    continue
                yield foreign_key

    def copy_expert(self, sql: str, file: IO[bytes], **kwargs: Any) -> None:
        with self.log_query(sql):
            cursor = self.get_cursor()
            cursor.copy_expert(sql, file, **kwargs)

    def export_to_csv(self, sql: str) -> bytes:
        """Exports the result of the given sql to CSV with a help of COPY statement."""
        with BytesIO() as output:
            self.copy_expert(f"COPY ({sql}) TO STDOUT WITH CSV HEADER", output)
            return output.getvalue()

    def get_search_path(self) -> str:
        search_path: str = self.run("show search_path;")[0]["search_path"]
        return search_path

    def restore_search_path(self, search_path: str) -> None:
        self.run(f"SELECT pg_catalog.set_config('search_path', '{search_path}', false);")

    def run_setup_file(self, sql: bytes) -> None:
        super().run_setup_file(PSQL_RESTRICT_RE.sub(b"", sql))

    def initial_setup(self, archive: zipfile.ZipFile) -> None:
        search_path = self.get_search_path()
        super().initial_setup(archive)
        self.restore_search_path(search_path)

    def recreate_database(self, owner: str | None = None) -> None:
        if owner is None:
            owner = self.user
        self.drop_connections(self.dbname)
        super().recreate_database(owner)

    def drop_connections(self, dbname: str) -> None:
        self.run(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s",
            [dbname],
            "maintenance",
        )

    def drop_database(self, dbname: str) -> None:
        self.run(f"DROP DATABASE IF EXISTS {dbname}", using="maintenance")

    def create_database(self, dbname: str, owner: str | None = None) -> None:
        self.run(
            f"CREATE DATABASE {dbname} WITH OWNER {owner}",
            using="maintenance",
        )

    def truncate(self) -> None:
        tables = [row["relname"] for row in self.run(TABLES_SQL)]
        self.run("TRUNCATE TABLE {} RESTART IDENTITY CASCADE".format(", ".join(tables)))

    def load_data_file(self, table_name: str, fd: IO[bytes]) -> None:
        self.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", fd)

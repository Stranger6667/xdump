import logging
import os
import zipfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from time import time
from typing import IO, Any

from .logging import get_logger

Row = dict[str, Any]
ForeignKey = dict[str, str]


class BaseBackend:
    connections: dict[str, dict[str, Any]] = {"default": {}}
    schema_filename = "dump/schema.sql"
    initial_setup_files: tuple[str, ...] = (schema_filename,)
    data_dir = "dump/data/"
    dbname: str
    verbosity: int

    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, "_logger"):
            self._logger = get_logger("XDump", self.verbosity)
        return self._logger

    @contextmanager
    def log_query(self, sql: str | bytes, params: Any = None) -> Iterator[None]:
        self.logger.debug("Execute query: %s", sql)
        self.logger.debug("Parameters: %s", str(params))
        with self.log_time():
            yield

    @contextmanager
    def log_time(self, message: str = "Execution time: %s") -> Iterator[None]:
        start = time()
        yield
        self.logger.debug(message, (time() - start))

    # Connection

    @property
    def _open_connections(self) -> dict[str, Any]:
        connections: dict[str, Any] = self.__dict__.setdefault("_open_connections_cache", {})
        return connections

    @property
    def _open_cursors(self) -> dict[str, Any]:
        cursors: dict[str, Any] = self.__dict__.setdefault("_open_cursors_cache", {})
        return cursors

    def get_connection(self, name: str = "default") -> Any:
        if name not in self._open_connections:
            self._open_connections[name] = self.connect(**self.connections[name])
        return self._open_connections[name]

    def get_cursor(self, name: str = "default") -> Any:
        if name not in self._open_cursors:
            self._open_cursors[name] = self.get_connection(name).cursor()
        return self._open_cursors[name]

    def connect(self, **kwargs: Any) -> Any:
        """Create a connection to the database."""
        raise NotImplementedError

    def get_connection_kwargs(self, **kwargs: Any) -> dict[str, Any]:
        for option in ("dbname", "user", "password", "host", "port"):
            kwargs.setdefault(option, getattr(self, option))
        return kwargs

    def cache_clear(self) -> None:
        """Closes all open connections."""
        self._open_cursors.clear()
        for connection in self._open_connections.values():
            connection.close()
        self._open_connections.clear()

    # Low-level commands executors

    def run_dump(self, *args: str) -> bytes:
        """Runs built-in utility for data / schema dumping."""
        raise NotImplementedError

    def run(self, sql: str | bytes, params: Any = None, using: str = "default") -> list[Row]:
        with self.log_query(sql, params):
            cursor = self.get_cursor(using)
            cursor.execute(sql, params)
            try:
                rows: list[Row] = cursor.fetchall()
                return rows
            except Exception as exc:
                self.handle_run_exception(exc)
                return []

    def handle_run_exception(self, exc: Exception) -> None:
        raise NotImplementedError

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Runs block of code inside a transaction."""
        self.run("BEGIN")
        yield
        self.run("COMMIT")

    # Dumping the data

    def dump(
        self,
        filename: str,
        full_tables: Sequence[str] = (),
        partial_tables: dict[str, str] | None = None,
        compression: int = zipfile.ZIP_DEFLATED,
        dump_schema: bool = True,
        dump_data: bool = True,
    ) -> None:
        """Creates a dump, which could be used to restore the database."""
        self.input_check(full_tables, partial_tables)
        with self.log_time("Total execution time: %s"):
            partial_tables = partial_tables or {}
            with zipfile.ZipFile(filename, "w", compression) as file:
                if dump_schema:
                    self.write_initial_setup(file)
                if dump_data:
                    self.add_related_data(full_tables, partial_tables)
                    self.write_full_tables(file, full_tables)
                    self.write_partial_tables(file, partial_tables)

    def input_check(self, full_tables: Sequence[str], partial_tables: dict[str, str] | None) -> None:
        if full_tables and partial_tables:
            common_tables = set(full_tables) & set(partial_tables)
            if common_tables:
                raise ValueError(
                    "`partial_tables` should not contain tables from `full_tables`. Common tables: {tables}".format(
                        tables=", ".join(common_tables)
                    )
                )

    def add_related_data(self, full_tables: Sequence[str], partial_tables: dict[str, str]) -> None:
        """Updates selects for partial tables to grab all objects, that are referenced by full / partial tables."""
        tables = self.get_tables_for_related_data(full_tables, partial_tables)
        for table in tables:
            self.update_partial_tables(table, full_tables, partial_tables)

    def get_tables_for_related_data(self, full_tables: Sequence[str], partial_tables: dict[str, str]) -> Sequence[str]:
        return tuple(full_tables) + tuple(partial_tables.keys())

    def update_partial_tables(self, table: str, full_tables: Sequence[str], partial_tables: dict[str, str]) -> None:
        self.update_recursive_relations(table, full_tables, partial_tables)
        self.update_non_recursive_relations(table, full_tables, partial_tables)

    def update_recursive_relations(
        self, table: str, full_tables: Sequence[str], partial_tables: dict[str, str]
    ) -> None:
        for foreign_key in self.get_foreign_keys(table, full_tables, recursive=True):
            if table in partial_tables:
                partial_tables[table] = RECURSIVE_QUERY_TEMPLATE.format(
                    source=partial_tables[table], target=table, **foreign_key
                )

    def update_non_recursive_relations(
        self, table: str, full_tables: Sequence[str], partial_tables: dict[str, str]
    ) -> None:
        for foreign_key in self.get_foreign_keys(table, full_tables):
            sql = self.get_related_data_sql(foreign_key, full_tables, partial_tables)
            if sql:
                foreign_table = foreign_key["foreign_table_name"]
                if foreign_table in partial_tables:
                    partial_tables[foreign_table] += " UNION " + sql
                else:
                    partial_tables[foreign_table] = sql
                # Now we select more than before for given table, so we have to do check related data for it.
                self.update_partial_tables(foreign_table, full_tables, partial_tables)

    def get_foreign_keys(
        self, table: str, full_tables: Sequence[str] = (), recursive: bool = False
    ) -> Iterator[ForeignKey]:
        """Looks for foreign keys in the given table. Excluding ones, that will be dumped in ``full_tables``."""
        raise NotImplementedError

    def get_related_data_sql(
        self, foreign_key: ForeignKey, full_tables: Sequence[str], partial_tables: dict[str, str]
    ) -> str | None:
        """Generates SQL to select related data, that is referred from another table."""
        table_name = foreign_key["table_name"]
        if table_name in full_tables:
            source = foreign_key["table_name"]
        elif table_name in partial_tables:
            source = f"({partial_tables[table_name]}) T"
        else:
            return None
        return """
            SELECT
                *
            FROM {foreign_table_name}
            WHERE {foreign_column_name} IN (
                SELECT {column_name} FROM {source}
            )""".format(source=source, **foreign_key)

    def write_initial_setup(self, file: zipfile.ZipFile) -> None:
        self.write_schema(file)

    def write_schema(self, file: zipfile.ZipFile) -> None:
        """Writes a DB schema, functions, etc to the archive."""
        schema = self.dump_schema()
        file.writestr(self.schema_filename, schema)

    def dump_schema(self) -> bytes:
        raise NotImplementedError

    def write_full_tables(self, file: zipfile.ZipFile, tables: Sequence[str]) -> None:
        """Writes a complete tables dump to the archive."""
        for table_name in tables:
            self.write_data_file(file, table_name, f"SELECT * FROM {table_name}")

    def write_partial_tables(self, file: zipfile.ZipFile, config: dict[str, str]) -> None:
        for table_name, sql in config.items():
            self.write_data_file(file, table_name, sql)

    def write_data_file(self, file: zipfile.ZipFile, table_name: str, sql: str) -> None:
        data = self.export_to_csv(sql)
        file.writestr(f"{self.data_dir}{table_name}.csv", data)

    def export_to_csv(self, sql: str) -> bytes:
        raise NotImplementedError

    # Database re-creation

    def recreate_database(self, owner: str | None = None) -> None:
        """Drops all connections to the database, drops the database and creates it again."""
        self.drop_database(self.dbname)
        self.create_database(self.dbname, owner)
        self.cache_clear()

    def drop_database(self, dbname: str) -> None:
        raise NotImplementedError

    def create_database(self, dbname: str, owner: str | None = None) -> None:
        raise NotImplementedError

    def truncate(self) -> None:
        """Truncates all tables in the DB. Alternative for the re-creation option."""
        raise NotImplementedError

    # Loading the dump

    def load(self, filename: str) -> None:
        """Loads schema, sequences and data into the database."""
        with self.log_time("Total execution time: %s"):
            archive = zipfile.ZipFile(filename)
            self.initial_setup(archive)
            self.load_data(archive)

    def initial_setup(self, archive: zipfile.ZipFile) -> None:
        """Loads schema and initial database configuration."""
        name_list = archive.namelist()
        for filename in self.initial_setup_files:
            if filename not in name_list:
                continue
            sql = archive.read(filename)
            self.run_setup_file(sql)

    def run_setup_file(self, sql: bytes) -> None:
        self.run(sql)

    def load_data(self, archive: zipfile.ZipFile) -> None:
        """Loads all data from data files inside the archive to the database."""
        with self.transaction():
            for name in archive.namelist():
                if name.startswith(self.data_dir):
                    fd = archive.open(name)
                    table_name = os.path.basename(name).split(".")[0]
                    self.load_data_file(table_name, fd)

    def load_data_file(self, table_name: str, fd: IO[bytes]) -> None:
        """Loads a data file into the database."""
        raise NotImplementedError


RECURSIVE_QUERY_TEMPLATE = """
WITH RECURSIVE recursive_cte AS (
  SELECT * FROM ({source}) S
  UNION
  SELECT T.*
  FROM {table_name} T
  INNER JOIN recursive_cte ON (recursive_cte.{column_name} = T.{foreign_column_name})
)
SELECT * FROM recursive_cte
"""

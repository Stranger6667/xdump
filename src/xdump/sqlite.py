import contextlib
import os
import sqlite3
import subprocess
import zipfile
from collections.abc import Iterator, Sequence
from csv import DictReader, DictWriter
from dataclasses import dataclass
from io import StringIO
from typing import IO, Any

from .base import BaseBackend, ForeignKey, Row


def dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> Row:
    return {description[0]: value for description, value in zip(cursor.description, row, strict=True)}


def force_string(value: str | bytes) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return value


TABLES_SQL = "SELECT name AS table_name FROM sqlite_master WHERE type='table'"


@dataclass(eq=False)
class SQLiteBackend(BaseBackend):
    dbname: str
    verbosity: int = 0

    def __post_init__(self) -> None:
        self.verbosity = int(self.verbosity)
        if sqlite3.sqlite_version_info < (3, 8, 3):
            raise RuntimeError(f"Minimum supported SQLite version is 3.8.3. You have {sqlite3.sqlite_version}")

    def connect(self, **kwargs: Any) -> sqlite3.Connection:
        connection = sqlite3.connect(self.dbname)
        connection.row_factory = dict_factory
        return connection

    def run_dump(self, *args: str) -> bytes:
        process = subprocess.Popen(("sqlite3",) + args, stdout=subprocess.PIPE)
        return process.communicate()[0]

    @property
    def tables(self) -> list[str]:
        return [result["table_name"] for result in self.run(TABLES_SQL)]

    def get_tables_for_related_data(self, full_tables: Sequence[str], partial_tables: dict[str, str]) -> Sequence[str]:
        # Return all tables for SQLite
        return self.tables

    def run(self, sql: str | bytes, params: Any = (), using: str = "default") -> list[Row]:
        return super().run(force_string(sql), params, using)

    def run_many(self, sql: str | bytes) -> None:
        with self.log_query(sql):
            cursor = self.get_cursor()
            cursor.executescript(force_string(sql))

    def begin_immediate(self) -> None:
        cursor = self.get_cursor()
        cursor.execute("BEGIN IMMEDIATE")

    def _get_foreign_keys(self, table: str) -> list[Row]:
        cache: dict[str, list[Row]] = self.__dict__.setdefault("_foreign_keys_cache", {})
        if table not in cache:
            cache[table] = self.run(f"PRAGMA foreign_key_list({table})")
        return cache[table]

    def get_foreign_keys(
        self, table: str, full_tables: Sequence[str] = (), recursive: bool = False
    ) -> Iterator[ForeignKey]:
        for foreign_key in self._get_foreign_keys(table):
            if foreign_key["table"] in full_tables:
                continue
            if foreign_key["table"] == table and not recursive:
                continue
            if foreign_key["table"] != table and recursive:
                continue
            yield {
                "foreign_table_name": foreign_key["table"],
                "table_name": table,
                "foreign_column_name": foreign_key["to"],
                "column_name": foreign_key["from"],
            }

    def dump(
        self,
        filename: str,
        full_tables: Sequence[str] = (),
        partial_tables: dict[str, str] | None = None,
        compression: int = zipfile.ZIP_DEFLATED,
        dump_schema: bool = True,
        dump_data: bool = True,
    ) -> None:
        self.input_check(full_tables, partial_tables)
        self.begin_immediate()
        super().dump(filename, full_tables, partial_tables, compression, dump_schema, dump_data)

    def dump_schema(self) -> bytes:
        return self.run_dump(self.dbname, ".schema")

    def export_to_csv(self, sql: str) -> bytes:
        cursor = self.get_cursor()
        with self.log_query(sql):
            cursor.execute(sql)
            data = cursor.fetchall()
        with StringIO() as output:
            writer = DictWriter(
                output,
                fieldnames=[column[0] for column in cursor.description],
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(data)
            return output.getvalue().encode()

    def drop_database(self, dbname: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.remove(dbname)

    def create_database(self, dbname: str, owner: str | None = None) -> None:
        sqlite3.connect(dbname).close()

    def truncate(self) -> None:
        for table in self.tables:
            self.run(f"DELETE FROM {table}")
        # No autoincrements are defined in the DB
        with contextlib.suppress(sqlite3.OperationalError):
            self.run("UPDATE sqlite_sequence SET seq=0")

    def run_setup_file(self, sql: bytes) -> None:
        self.run_many(sql)

    def load_data(self, archive: zipfile.ZipFile) -> None:
        """Loads all data from data files inside the archive to the database."""
        for name in archive.namelist():
            if name.startswith(self.data_dir):
                fd = archive.open(name)
                filename = os.path.basename(name).split(".")[0]
                self.load_data_file(filename, fd)
        with contextlib.suppress(sqlite3.OperationalError):
            self.run("COMMIT")

    def load_data_file(self, table_name: str, fd: IO[bytes]) -> None:
        reader = DictReader(fd.read().decode().split("\n"), delimiter=",")
        fieldnames = reader.fieldnames or []
        fields = ",".join(fieldnames)
        placeholders = ("?," * len(fieldnames))[:-1]
        cursor = self.get_cursor()
        cursor.executemany(
            f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders})",
            [[line[k] for k in fieldnames] for line in reader],
        )

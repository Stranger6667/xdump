import contextlib
import os
import sqlite3
import subprocess
from csv import DictReader, DictWriter
from dataclasses import dataclass
from io import StringIO

from .base import BaseBackend


def dict_factory(cursor, row):
    return {description[0]: value for description, value in zip(cursor.description, row, strict=True)}


def force_string(value):
    if isinstance(value, bytes):
        value = value.decode()
    return value


TABLES_SQL = "SELECT name AS table_name FROM sqlite_master WHERE type='table'"


@dataclass(eq=False)
class SQLiteBackend(BaseBackend):
    dbname: str
    verbosity: int = 0

    def __post_init__(self):
        self.verbosity = int(self.verbosity)
        if sqlite3.sqlite_version_info < (3, 8, 3):
            raise RuntimeError(f"Minimum supported SQLite version is 3.8.3. You have {sqlite3.sqlite_version}")

    def connect(self, *args, **kwargs):
        connection = sqlite3.connect(self.dbname)
        connection.row_factory = dict_factory
        return connection

    def run_dump(self, *args, **kwargs):
        process = subprocess.Popen(("sqlite3",) + args, stdout=subprocess.PIPE)
        return process.communicate()[0]

    @property
    def tables(self):
        return [result["table_name"] for result in self.run(TABLES_SQL)]

    def get_tables_for_related_data(self, full_tables, partial_tables):
        # Return all tables for SQLite
        return self.tables

    def run(self, sql, params=(), using="default"):
        sql = force_string(sql)
        return super().run(sql, params, using)

    def run_many(self, sql):
        with self.log_query(sql):
            sql = force_string(sql)
            cursor = self.get_cursor()
            cursor.executescript(sql)

    def begin_immediate(self):
        cursor = self.get_cursor()
        cursor.execute("BEGIN IMMEDIATE")

    def _get_foreign_keys(self, table):
        cache = self.__dict__.setdefault("_foreign_keys_cache", {})
        if table not in cache:
            cache[table] = self.run(f"PRAGMA foreign_key_list({table})")
        return cache[table]

    def get_foreign_keys(self, table, full_tables=(), recursive=False):
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

    def dump(self, filename, full_tables=(), partial_tables=None, **kwargs):
        self.input_check(full_tables, partial_tables)
        self.begin_immediate()
        super().dump(filename, full_tables=full_tables, partial_tables=partial_tables, **kwargs)

    def dump_schema(self):
        return self.run_dump(self.dbname, ".schema")

    def export_to_csv(self, sql):
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

    def drop_database(self, dbname):
        with contextlib.suppress(FileNotFoundError):
            os.remove(dbname)

    def create_database(self, dbname, *args, **kwargs):
        sqlite3.connect(dbname).close()

    def truncate(self):
        for table in self.tables:
            self.run(f"DELETE FROM {table}")
        # No autoincrements are defined in the DB
        with contextlib.suppress(sqlite3.OperationalError):
            self.run("UPDATE sqlite_sequence SET seq=0")

    def run_setup_file(self, sql):
        self.run_many(sql)

    def load_data(self, archive):
        """Loads all data from data files inside the archive to the database."""
        for name in archive.namelist():
            if name.startswith(self.data_dir):
                fd = archive.open(name)
                filename = os.path.basename(name).split(".")[0]
                self.load_data_file(filename, fd)
        with contextlib.suppress(sqlite3.OperationalError):
            self.run("COMMIT")

    def load_data_file(self, table_name, fd):
        reader = DictReader(fd.read().decode().split("\n"), delimiter=",")
        fields = ",".join(reader.fieldnames)
        placeholders = ("?," * len(reader.fieldnames))[:-1]
        cursor = self.get_cursor()
        cursor.executemany(
            f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders})",
            [[line[k] for k in reader.fieldnames] for line in reader],
        )

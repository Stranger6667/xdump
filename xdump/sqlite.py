# coding: utf-8
import sqlite3
import subprocess
from csv import DictReader
from pathlib import Path

from .base import BaseBackend


def dict_factory(cursor, row):
    return {description[0]: value for description, value in zip(cursor.description, row)}


def force_string(value):
    if isinstance(value, bytes):
        value = value.decode()
    return value


class SQLiteBackend(BaseBackend):

    def connect(self, *args, **kwargs):
        connection = sqlite3.connect(self.dbname)
        connection.row_factory = dict_factory
        return connection

    def run_dump(self, *args, **kwargs):
        process = subprocess.Popen(['sqlite3', *args], stdout=subprocess.PIPE)
        return process.communicate()[0]

    def run(self, sql, params=(), using='default'):
        sql = force_string(sql)
        return super().run(sql, params, using)

    def run_many(self, sql):
        sql = force_string(sql)
        cursor = self.get_cursor()
        cursor.executescript(sql)

    def dump_schema(self):
        return self.run_dump(self.dbname, '.schema')

    def export_to_csv(self, sql):
        # TODO. SQLite could produce empty CSV files, without headers if there is no data.
        # Probably, the should not be included in the dump at all.
        # It seems like not possible to run everything in on transaction.
        # Probably it could be batched
        # Another approach - dump it in Python
        return self.run_dump('-header', '-csv', self.dbname, sql)

    def drop_database(self, dbname):
        try:
            Path(dbname).unlink()
        except FileNotFoundError:
            pass

    def create_database(self, dbname, *args, **kwargs):
        with sqlite3.connect(dbname):
            pass

    def run_setup_file(self, sql):
        self.run_many(sql)

    def load_data(self, archive):
        """
        Loads all data from data files inside the archive to the database.
        """
        for name in archive.namelist():
            if name.startswith(self.data_dir):
                fd = archive.open(name)
                filename = Path(name).stem
                self.load_data_file(filename, fd)

    def load_data_file(self, table_name, fd):
        reader = DictReader(fd.read().decode().split('\n'), delimiter=',')
        fields = ','.join(reader.fieldnames)
        placeholders = ('?,' * len(reader.fieldnames))[:-1]
        cursor = self.get_cursor()
        cursor.executemany(
            f'INSERT INTO {table_name} ({fields}) VALUES ({placeholders})',
            [[line[k] for k in reader.fieldnames] for line in reader]
        )

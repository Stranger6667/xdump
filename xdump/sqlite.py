# coding: utf-8
import sqlite3
import subprocess
from csv import DictReader
from pathlib import Path

from .base import BaseBackend


class SQLiteBackend(BaseBackend):

    def connect(self, *args, **kwargs):
        return sqlite3.connect(self.dbname)

    def run_dump(self, *args, **kwargs):
        process = subprocess.Popen(['sqlite3', *args], stdout=subprocess.PIPE)
        return process.communicate()[0]

    def run(self, sql, params=None, using='default'):
        if isinstance(sql, bytes):
            sql = sql.decode()
        cursor = self.get_cursor(using)
        cursor.executescript(sql)
        try:
            return cursor.fetchall()
        except Exception as exc:
            self.handle_run_exception(exc)

    def run2(self, sql, params=(), using='default'):
        if isinstance(sql, bytes):
            sql = sql.decode()
        cursor = self.get_cursor(using)
        cursor.execute(sql, params)
        try:
            return cursor.fetchall()
        except Exception as exc:
            self.handle_run_exception(exc)

    def dump_schema(self):
        return self.run_dump(self.dbname, '.schema')

    def export_to_csv(self, sql):
        # TODO. SQLite could produce empty CSV files, without headers if there is no data.
        # Probably, the should not be included in the dump at all.
        # It seems like not possible to run everything in on transaction.
        # Probably it could be batched
        return self.run_dump('-header', '-csv', self.dbname, sql)

    def drop_database(self, dbname):
        try:
            Path(dbname).unlink()
        except ValueError:
            pass

    def create_database(self, dbname, *args, **kwargs):
        with sqlite3.connect(dbname):
            pass

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
        sql = f'INSERT INTO {table_name} ({fields}) VALUES ({placeholders})'
        for line in reader:
            v = [line[k] for k in reader.fieldnames]
            self.run2(sql, v)

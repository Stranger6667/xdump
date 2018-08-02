# coding: utf-8
import os
import sqlite3
import subprocess
import sys
from csv import DictReader, DictWriter

from ._compat import FileNotFoundError, StringIO, lru_cache
from .base import BaseBackend


def dict_factory(cursor, row):
    return {description[0]: value for description, value in zip(cursor.description, row)}


def force_string(value):
    if isinstance(value, bytes):
        value = value.decode()
    return value


class SQLiteBackend(BaseBackend):

    def __init__(self, *args, **kwargs):
        if sqlite3.sqlite_version_info < (3, 8, 3):
            raise RuntimeError('Minimum supported SQLite version is 3.8.3. You have {0}'.format(sqlite3.sqlite_version))
        super(SQLiteBackend, self).__init__(*args, **kwargs)

    def connect(self, *args, **kwargs):
        connection = sqlite3.connect(self.dbname)
        connection.row_factory = dict_factory
        return connection

    def run_dump(self, *args, **kwargs):
        process = subprocess.Popen(('sqlite3', ) + args, stdout=subprocess.PIPE)
        return process.communicate()[0]

    def get_tables_for_related_data(self, full_tables, partial_tables):
        for result in self.run("SELECT name AS table_name FROM sqlite_master WHERE type='table'"):
            yield result['table_name']

    def run(self, sql, params=(), using='default'):
        sql = force_string(sql)
        return super(SQLiteBackend, self).run(sql, params, using)

    def run_many(self, sql):
        with self.log_query(sql):
            sql = force_string(sql)
            cursor = self.get_cursor()
            cursor.executescript(sql)

    def begin_immediate(self):
        cursor = self.get_cursor()
        cursor.execute('BEGIN IMMEDIATE')

    @lru_cache()
    def _get_foreign_keys(self, table):
        return self.run('PRAGMA foreign_key_list({})'.format(table))

    def get_foreign_keys(self, table, full_tables=(), recursive=False):
        for foreign_key in self._get_foreign_keys(table):
            if foreign_key['table'] in full_tables:
                continue
            if foreign_key['table'] == table and not recursive:
                continue
            if foreign_key['table'] != table and recursive:
                continue
            yield {
                'foreign_table_name': foreign_key['table'],
                'table_name': table,
                'foreign_column_name': foreign_key['to'],
                'column_name': foreign_key['from'],
            }
        if sys.version_info[:2] < (3, 6):
            # Before 3.6 sqlite3 used to implicitly commit an open transaction in this case.
            self.begin_immediate()

    def dump(self, filename, full_tables=(), partial_tables=None, **kwargs):
        self.input_check(full_tables, partial_tables)
        self.begin_immediate()
        super(SQLiteBackend, self).dump(filename, full_tables=full_tables, partial_tables=partial_tables, **kwargs)

    def dump_schema(self):
        return self.run_dump(self.dbname, '.schema')

    def export_to_csv(self, sql):
        output = StringIO()
        cursor = self.get_cursor()
        with self.log_query(sql):
            cursor.execute(sql)
            data = cursor.fetchall()
        writer = DictWriter(output, fieldnames=[column[0] for column in cursor.description], lineterminator='\n')
        writer.writeheader()
        writer.writerows(data)
        result = output.getvalue().encode()
        output.close()  # StringIO doesn't support context manager protocol on Python 2
        return result

    def drop_database(self, dbname):
        try:
            os.remove(dbname)
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
                filename = os.path.basename(name).split('.')[0]
                self.load_data_file(filename, fd)

    def load_data_file(self, table_name, fd):
        reader = DictReader(fd.read().decode().split('\n'), delimiter=',')
        fields = ','.join(reader.fieldnames)
        placeholders = ('?,' * len(reader.fieldnames))[:-1]
        cursor = self.get_cursor()
        cursor.executemany(
            'INSERT INTO {0} ({1}) VALUES ({2})'.format(table_name, fields, placeholders),
            [[line[k] for k in reader.fieldnames] for line in reader]
        )

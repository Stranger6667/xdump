# coding: utf-8
import zipfile
from functools import lru_cache

import attr


BACKENDS = {
    'postgres': 'xdump.backends.postgresql',
}


@attr.s(cmp=False)
class DatabaseWrapper:
    backend = attr.ib()
    dbname = attr.ib()
    user = attr.ib()
    password = attr.ib()
    host = attr.ib()
    port = attr.ib()

    @lru_cache()
    def get_backend(self):
        import_string = BACKENDS[self.backend]
        klass = __import__(import_string, fromlist=['Backend']).Backend
        return klass(self.dbname, self.user, self.password, self.host, self.port)

    def dump(self, filename, full_tables=(), partial_tables=None):
        """
        Creates a dump, which could be used to restore the database.
        """
        self.get_backend().dump(filename, full_tables, partial_tables)

    def populate_database(self, filename):
        """
        Recreates the database with data from the archive.
        """
        self.get_backend().populate_database(filename)

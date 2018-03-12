# coding: utf-8
from ..core import XDumpCommand


class Command(XDumpCommand):
    help = 'Loads an SQL dump.'

    def _handle(self, filename, backend):
        backend.recreate_database()
        backend.load(filename)

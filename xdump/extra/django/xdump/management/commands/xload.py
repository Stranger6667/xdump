# coding: utf-8
from ..core import XDumpCommand


class Command(XDumpCommand):
    help = 'Loads an SQL dump.'

    def handle(self, filename, **options):
        backend = self.get_xdump_backend(options['alias'], options['backend'], 'postgres')
        owner = self.get_database_configuration(options['alias']).get('USER')
        backend.recreate_database(owner)
        backend.load(filename)

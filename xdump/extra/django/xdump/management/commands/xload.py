# coding: utf-8
from ..core import XDumpCommand


class Command(XDumpCommand):
    help = 'Loads an SQL dump.'

    def add_arguments(self, parser):
        super(Command, self).add_arguments(parser)
        parser.add_argument(
            '-t', '--truncate',
            action='store',
            dest='truncate',
            help='Truncates tables instead of DB re-creation.',
            required=False,
            default=False,
        )

    def _handle(self, filename, backend, **options):
        if options['truncate']:
            backend.truncate()
        elif not backend.is_dump_without_schema(filename):
            backend.recreate_database()
        backend.load(filename)

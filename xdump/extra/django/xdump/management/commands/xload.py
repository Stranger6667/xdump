# coding: utf-8
from ..core import XDumpCommand


class Command(XDumpCommand):
    help = 'Loads an SQL dump.'

    def add_arguments(self, parser):
        super(Command, self).add_arguments(parser)
        parser.add_argument(
            '-m', '--cleanup-method',
            action='store',
            nargs='?',
            choices=['recreate', 'truncate'],
            dest='cleanup_method',
            help='Method of DB cleaning up',
            required=False,
        )

    def _handle(self, filename, backend, **options):
        if options['cleanup_method'] == 'truncate':
            backend.truncate()
        elif options['cleanup_method'] == 'recreate':
            backend.recreate_database()
        backend.load(filename)

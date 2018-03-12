# coding: utf-8
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.module_loading import import_string


class XDumpCommand(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            'filename',
            action='store',
            help='Path to dump zip file.',
        )
        parser.add_argument(
            '-a', '--alias',
            action='store',
            dest='alias',
            help='Database configuration alias.',
            required=False,
            default='default',
        )
        parser.add_argument(
            '-b', '--backend',
            action='store',
            dest='backend',
            help='Importable string for custom XDump backend.',
            required=False,
            default=None,
        )

    def handle(self, filename, **options):
        backend = self.get_xdump_backend(options['alias'], options['backend'])
        self._handle(filename, backend)

    def _handle(self, filename, backend):
        raise NotImplementedError

    def get_xdump_backend(self, alias='default', backend=None):
        configuration = self.get_database_configuration(alias)
        if backend is None:
            if 'BACKEND' in settings.XDUMP:
                backend = settings.XDUMP['BACKEND']
            else:
                backend = {
                    'django.db.backends.postgresql': 'xdump.postgresql.PostgreSQLBackend',
                    'django.db.backends.postgresql_psycopg2': 'xdump.postgresql.PostgreSQLBackend',
                    'django.db.backends.sqlite': 'xdump.sqlite.SQLiteBackend',
                }[configuration['ENGINE']]
        backend_class = import_string(backend)
        return backend_class(
            dbname=configuration['NAME'],
            user=configuration.get('USER'),
            password=configuration.get('PASSWORD'),
            host=configuration.get('HOST'),
            port=configuration.get('PORT'),
        )

    def get_database_configuration(self, alias):
        return settings.DATABASES[alias]

    def get_dump_kwargs(self):
        return {
            'full_tables': settings.XDUMP['FULL_TABLES'],
            'partial_tables': settings.XDUMP['PARTIAL_TABLES'],
        }

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

    def handle(self, filename, alias, backend, verbosity, **options):
        backend = self.get_xdump_backend(alias, backend, verbosity)
        self._handle(filename, backend, **options)

    def _handle(self, filename, backend, **options):
        raise NotImplementedError

    def get_xdump_backend(self, alias='default', backend=None, verbosity=0):
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
        return _init_backend(
            backend,
            dbname=configuration['NAME'],
            user=configuration.get('USER'),
            password=configuration.get('PASSWORD'),
            host=configuration.get('HOST'),
            port=configuration.get('PORT'),
            verbosity=verbosity,
        )

    def get_database_configuration(self, alias):
        return settings.DATABASES[alias]

    def get_dump_kwargs(self):
        return {
            'full_tables': settings.XDUMP['FULL_TABLES'],
            'partial_tables': settings.XDUMP['PARTIAL_TABLES'],
        }


def _init_backend(path, **kwargs):
    backend_class = import_string(path)
    init_kwargs = {attr.name: kwargs[attr.name] for attr in backend_class.__attrs_attrs__}
    return backend_class(**init_kwargs)

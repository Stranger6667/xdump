import inspect
from typing import Any

from django.conf import settings
from django.core.management.base import BaseCommand, CommandParser
from django.utils.module_loading import import_string

from xdump.base import BaseBackend


class XDumpCommand(BaseCommand):
    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "filename",
            action="store",
            help="Path to dump zip file.",
        )
        parser.add_argument(
            "-a",
            "--alias",
            action="store",
            dest="alias",
            help="Database configuration alias.",
            required=False,
            default="default",
        )
        parser.add_argument(
            "-b",
            "--backend",
            action="store",
            dest="backend",
            help="Importable string for custom XDump backend.",
            required=False,
            default=None,
        )

    def handle(self, *args: Any, **options: Any) -> None:
        backend = self.get_xdump_backend(options.pop("alias"), options.pop("backend"), options.pop("verbosity"))
        try:
            self._handle(options.pop("filename"), backend, **options)
        finally:
            backend.cache_clear()

    def _handle(self, filename: str, backend: BaseBackend, **options: Any) -> None:
        raise NotImplementedError

    def get_xdump_backend(self, alias: str = "default", backend: str | None = None, verbosity: int = 0) -> BaseBackend:
        configuration = self.get_database_configuration(alias)
        if backend is None:
            if "BACKEND" in settings.XDUMP:
                backend = settings.XDUMP["BACKEND"]
            else:
                backend = {
                    "django.db.backends.postgresql": "xdump.postgresql.PostgreSQLBackend",
                    "django.db.backends.sqlite3": "xdump.sqlite.SQLiteBackend",
                }[configuration["ENGINE"]]
        return _init_backend(
            backend,
            dbname=configuration["NAME"],
            user=configuration.get("USER"),
            password=configuration.get("PASSWORD"),
            host=configuration.get("HOST"),
            port=configuration.get("PORT"),
            verbosity=verbosity,
        )

    def get_database_configuration(self, alias: str) -> dict[str, Any]:
        configuration: dict[str, Any] = settings.DATABASES[alias]
        return configuration

    def get_dump_kwargs(self) -> dict[str, Any]:
        return {
            "full_tables": settings.XDUMP["FULL_TABLES"],
            "partial_tables": settings.XDUMP["PARTIAL_TABLES"],
        }


def _init_backend(path: str, **kwargs: Any) -> BaseBackend:
    backend_class: type[BaseBackend] = import_string(path)
    init_kwargs = {name: kwargs[name] for name in inspect.signature(backend_class).parameters}
    return backend_class(**init_kwargs)

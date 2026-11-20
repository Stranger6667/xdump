import itertools
from collections.abc import Iterable, Iterator


def make_options(option_key: str, container: Iterable[str]) -> Iterator[str]:
    """Creates a list of options from the given list of values."""
    return itertools.chain.from_iterable([(option_key, value) for value in container])

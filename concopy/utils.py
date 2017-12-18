# coding: utf-8


def flatten(container):
    return sum(container, ())


def make_options(container, option_key):
    """
    Creates a list of options from the given list of values.
    """
    return flatten([(option_key, value) for value in container])

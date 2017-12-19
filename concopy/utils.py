# coding: utf-8
import itertools


def make_options(option_key, container):
    """
    Creates a list of options from the given list of values.
    """
    return itertools.chain.from_iterable([(option_key, value) for value in container])

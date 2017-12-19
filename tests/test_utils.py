from concopy.utils import make_options


def test_make_options():
    assert list(make_options('-t', ['foo', 'bar'])) == [
        '-t', 'foo', '-t', 'bar'
    ]

import pytest
from alpaca2pg.tutorial import run


@pytest.fixture
def options() -> dict:
    """Bonobo options"""
    d = dict()
    return d


def test_run(options, context):
    run(**options)

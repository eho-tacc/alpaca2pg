import pytest
from alpaca2pg.tutorial import run


@pytest.fixture
def options():
    """Bonobo options"""
    return dict()


def test_main(options):
    run(**options)

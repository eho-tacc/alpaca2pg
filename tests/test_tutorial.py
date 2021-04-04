import pytest
import json
from pprint import pprint as pp
from alpaca2pg.tutorial import run


@pytest.fixture
def options() -> dict:
    """Bonobo options"""
    d = dict()
    return d


def test_run(options):
    run(**options)

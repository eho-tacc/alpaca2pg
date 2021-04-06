import pytest
from alpaca2pg.__main__ import main


def test_fixture_pg_conn(pg_conn):
    _ = pg_conn


def test_main(pg_conn):
    """description"""
    result = main(pg_conn)
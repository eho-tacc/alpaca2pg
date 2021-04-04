import os
import pytest
import json
from pprint import pprint as pp
from alpaca2pg.tutorial import run
from dotenv import dotenv_values


@pytest.fixture
def dot_env(monkeypatch) -> dict:
    return dotenv_values()


@pytest.fixture
def context(dot_env, monkeypatch):
    """Emulates bonobo run --env-file .env"""
    for k, v in dot_env.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def options() -> dict:
    """Bonobo options"""
    d = dict()
    return d


def test_run(options, context):
    run(**options)

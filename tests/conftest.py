import os
import pytest
import psycopg2
from dotenv import dotenv_values


@pytest.fixture
def dot_env() -> dict:
    """.env by default"""
    return dotenv_values()


@pytest.fixture
def env_context(dot_env, monkeypatch):
    """Get env file .env"""
    for k, v in dot_env.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def pg_conn(env_context):
    """Connect to PostgreSQL DB using URI in .env file"""
    uri = os.getenv('ALP2PG_DB_URI')
    assert uri is not None
    conn = psycopg2.connect(uri)
    yield conn
    conn.close()

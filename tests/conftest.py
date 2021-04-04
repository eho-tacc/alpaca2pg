import pytest
from dotenv import dotenv_values


@pytest.fixture
def dot_env() -> dict:
    """.env by default"""
    return dotenv_values()


@pytest.fixture
def env_context(dot_env, monkeypatch):
    """Emulates bonobo run --env-file .env"""
    for k, v in dot_env.items():
        monkeypatch.setenv(k, v)

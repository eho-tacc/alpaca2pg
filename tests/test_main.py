import pytest
from alpaca2pg.__main__ import main


def test_can_run(env_context):
    result = main()


    
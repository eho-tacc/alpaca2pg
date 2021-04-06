import pytest
import bonobo
from bonobo.execution.contexts import (
    NodeExecutionContext as NEC,
    GraphExecutionContext as GEC)
from alpaca2pg.tutorial import get_graph, get_services, extract_bars_iter


@pytest.fixture
def options() -> dict:
    """Bonobo options"""
    d = dict()
    return d


def test_extract_bars_iter(env_context):
    with NEC(extract_bars_iter, services=get_services()) as context:
        # Write a list of rows, including BEGIN/END control messages.
        context.write_sync(
            'AAPL',
            'TSLA'
        )
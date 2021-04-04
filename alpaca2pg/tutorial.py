import os
import bonobo
from alpaca_trade_api.rest import REST as AlpacaREST




def extract():
    """Placeholder, change, rename, remove... """
    yield 'hello'
    yield 'world'


def transform(*args):
    """Placeholder, change, rename, remove... """
    yield tuple(
        map(str.title, args)
    )


def load(*args):
    """Placeholder, change, rename, remove... """
    print(*args)


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph
    """
    graph = bonobo.Graph()
    graph.add_chain(
        extract, transform, load
    )
    return graph


def get_alpaca_service():
    """Authenticates and returns an Alpaca REST interface"""
    ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
    return AlpacaREST(key_id=ALPACA_KEY_ID, secret_key=ALPACA_SECRET_KEY)

def get_services(**options) -> dict:
    return dict(
        alpaca=get_alpaca_service()
    )


def run(**options):
    bonobo.run(
        get_graph(**options),
        services=get_services(**options)
    )


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    """Main entrypoint"""
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        run(**options)
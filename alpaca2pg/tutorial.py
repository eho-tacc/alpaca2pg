import os
from datetime import datetime as dt, date
from pdb import set_trace as st
import bonobo
from bonobo.config import use
from alpaca_trade_api.rest import REST as AlpacaREST, TimeFrame


@use('alpaca')
def extract_bars_iter(alpaca, *rows):
    """Placeholder, change, rename, remove... """
    bar_iter = alpaca.get_bars_iter(
        symbol="AAPL", 
        timeframe=TimeFrame.Minute, 
        start="2021-02-08", 
        end="2021-02-09", 
        adjustment='raw',
        limit=10, 
        raw=True)
    for bar in bar_iter:
        yield bar


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
        extract_bars_iter, 
        bonobo.PrettyPrinter()
    )
    return graph


def get_alpaca_service(key_env_name="ALPACA_KEY_ID",
                       secret_env_name="ALPACA_SECRET_KEY"):
    """Authenticates and returns an Alpaca REST interface"""
    base_url = 'https://paper-api.alpaca.markets/'
    return AlpacaREST(key_id=os.getenv(key_env_name), 
                      secret_key=os.getenv(secret_env_name),
                      base_url=base_url)


def get_services(**options) -> dict:
    return dict(
        alpaca=get_alpaca_service()
    )


def run(**options):
    bonobo.run(
        get_graph(**options),
        services=get_services(**options)
    )


def get_parser():
    """Bonobo argument parser"""
    parser = bonobo.get_argument_parser()
    parser.add_argument('--ticker', help='ticker to pull',
                        required=True)
    parser.add_argument('--start-date', help='start date',
                        required=True, type=lambda s: dt.strptime(s, '%Y-%m-%d').date())
    parser.add_argument('--end-date', help='end date',
                        required=False, type=lambda s: dt.strptime(s, '%Y-%m-%d').date(),
                        default=dt.today().date())
    return parser


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = get_parser()
    with bonobo.parse_args(parser) as options:
        run(**options)
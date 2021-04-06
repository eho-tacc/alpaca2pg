import os
from datetime import datetime as dt, date
from pdb import set_trace as st
import bonobo
from bonobo.config import use
from alpaca_trade_api.rest import REST as AlpacaREST, TimeFrame


def getenv(name) -> str:
    """Fault intolerant getter from env"""
    val = os.getenv(name)
    if val is None:
        raise bonobo.errors.UnrecoverableError(
            f"Required input parameter `{name}` was not found as env var")
    return val


def env2date(env_name) -> date:
    yield dt.strptime(getenv(env_name), '%Y-%m-%d').date()


def env2timeframe(env_name='ALP2PG_TF'):
    tf = getenv(env_name)
    try:
        return getattr(TimeFrame, tf)
    except:
        raise bonobo.errors.UnrecoverableError(
            f"Invalid TimeFrame provided: {env_name}={tf}")


@use('alpaca')
def extract_bars_iter(alpaca):
    """Get bars using Alpaca REST API"""
    try:
        bar_iter = alpaca.get_bars_iter(
            symbol=getenv("ALP2PG_SYMBOL"), 
            timeframe=env2timeframe(), 
            start=env2date("ALP2PG_START"), 
            end=env2date("ALP2PG_END"), 
            adjustment='raw',
            # limit=10, 
            raw=True)
    except:
        # TODO: handling
        raise
    for bar in bar_iter:
        yield bar


def get_graph(**options):
    """Get graph"""
    g = bonobo.Graph()

    # Extraction pipeline for each ticker symbol
    g.add_chain(
        extract_bars_iter, 
        bonobo.Limit(5), 
        bonobo.PrettyPrinter(),
        _name='pull_ts')

    return g


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


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        run(**options)
import os
import argparse
import pandas as pd
import petl
from datetime import datetime as dt, date
from pdb import set_trace as st
import logging
from alpaca_trade_api.rest import REST as AlpacaREST, TimeFrame as TF
from utils import getenv
from pgutils import get_pg_conn, table_exists

logging.basicConfig(level=logging.INFO)


def safe_append(data, cur, tab_name):
    """Append `data` to table `tab_name`. Create table if it
    does not exist.
    """
    if table_exists(cur, tab_name):
        petl.appenddb(data, cur, tab_name)
    else:
        petl.todb(data, cur, tab_name, create=True, dialect='postgresql')


def get_alpaca_client():
    return AlpacaREST(key_id=getenv('ALPACA_KEY_ID'), 
                      secret_key=getenv('ALPACA_SECRET_KEY'), 
                      base_url=getenv('ALPACA_URL'))


def get_tab_name(ticker, timeframe, sep='.'):
    """description"""
    return f"{ticker}{sep}1{sep}{timeframe.lower()}"


def get_alpaca_bars(ticker, timeframe, start_date, end_date) -> pd.DataFrame:
    return (
        get_alpaca_client()
        .get_bars(
            symbol=ticker, 
            timeframe=getattr(TF, timeframe),
            start=start_date, 
            end=end_date, 
            adjustment='raw')
        .df
        .reset_index()
        .rename(columns=dict(timestamp='time'))
    )


def main(**opts):
    """Main entrypoint function"""
    df = get_alpaca_bars(**opts)
    table = petl.fromdataframe(df)

    # Append data to table
    cur = get_pg_conn().cursor()
    tab_name = get_tab_name(opts['ticker'], opts['timeframe'])
    safe_append(table, cur, tab_name)


def get_opts():
    """Get command line options from argparse"""
    p = argparse.ArgumentParser(
        description=("Pull historical quotes from Alpaca REST API to "
                     "PostgreSQL DB. Please see `pysopg2.connect` docs "
                     "for details."))
    p.add_argument('-t', '--ticker', type=str, required=True)
    p.add_argument('-f', '--timeframe', type=str, required=True,
                   choices=['Minute', 'Hour', 'Day'])
    p.add_argument('-s', '--start-date', help='start date', required=True, 
                   type=lambda s: dt.strptime(s, '%Y-%m-%d').date())
    p.add_argument('-e', '--end-date', help='end date', required=False, 
                   type=lambda s: dt.strptime(s, '%Y-%m-%d').date(), 
                   default=dt.today().date())
    return p.parse_args()


if __name__ == '__main__':
    opts = get_opts()
    main(**opts)
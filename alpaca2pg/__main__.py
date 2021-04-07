import os
import argparse
import petl
import psycopg2
import pkg_resources
from datetime import date, datetime as dt
from pdb import set_trace as st
import logging

logging.basicConfig(level=logging.INFO)


def get_pg_uri(user, password, host, port, dbname) -> str:
    """Returns PostgreSQL URI-formatted string."""
    return f'postgresql://{user}:{password}@{host}:{port}/{dbname}'


def get_pg_conn(opts):
    """Connect to remote DB using credentials passed in command line"""
    kw = {k: getenv(f"PG_{k.upper()}") for k in 
          ('dbname', 'user', 'password', 'host', 'port')}
    uri = get_pg_uri(**kw)
    return psycopg2.connect(uri)


def get_sql(fname, sql_dir='sql') -> str:
    """Reads SQL query from file at `sql_dir`/`fname`."""
    fp = pkg_resources.resource_filename('alpaca2pg', os.path.join(sql_dir, fname))
    with open(fp, 'r', encoding='utf-8') as f:
        return f.read()


def safe_append(data, cur, tab_name):
    """Append `data` to table `tab_name`. Create table if it
    does not exist.
    """
    if table_exists(cur, tab_name):
        petl.appenddb(data, cur, tab_name)
    else:
        petl.todb(data, cur, tab_name, create=True)


def table_exists(cur, tab_name) -> bool:
    cur.execute(get_sql('table_exists.sql'), (tab_name,))
    return bool(cur.fetchone()[0])


def getenv(name, permissive=False):
    """Fault-intolerant fetcher from env."""
    val = os.getenv(name)
    if val is None and not permissive:
        raise ValueError(f"Missing required input parameter from env: {name}")
    return val


def main(**opts):
    """Main entrypoint function"""
    cur = get_pg_conn(opts).cursor()
    data = [['foo', 'bar'], 
            ['a', 1], 
            ['b', 2], 
            ['c', 2]]
    tab_name = 'foobar'

    # Append data
    safe_append(data, cur, tab_name)


def get_opts():
    """Get command line options from argparse"""
    p = argparse.ArgumentParser(
        description=("Pull historical quotes from Alpaca REST API to "
                     "PostgreSQL DB. Please see `pysopg2.connect` docs "
                     "for details."))
    p.add_argument('-t', '--ticker', type=str, required=True)
    p.add_argument('-s', '--start-date', help='start date', required=True, 
                   type=lambda s: dt.strptime(s, '%Y-%m-%d').date())
    p.add_argument('-e', '--end-date', help='end date', required=False, 
                   type=lambda s: dt.strptime(s, '%Y-%m-%d').date(), 
                   default=dt.today().date())
    return vars(p.parse_args())


if __name__ == '__main__':
    opts = get_opts()
    main(**opts)
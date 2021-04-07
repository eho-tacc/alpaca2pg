import os
import argparse
import petl
import psycopg2
import pkg_resources
from pdb import set_trace as st
import logging

logging.basicConfig(level=logging.DEBUG)


def get_pg_uri(user, password, host, port, dbname) -> str:
    """Returns PostgreSQL URI-formatted string."""
    return f'postgresql://{user}:{password}@{host}:{port}/{dbname}'


def get_pg_conn(opts):
    """Connect to remote DB using credentials passed in command line"""
    kw = {k: getattr(opts, k) for k in 
          ('dbname', 'user', 'password', 'host', 'port')}
    return psycopg2.connect(get_pg_uri(**kw))


def get_sql(fname, sql_dir='sql') -> str:
    """Reads SQL query in file at `fname`."""
    fp = pkg_resources.resource_filename('alpaca2pg', os.path.join(sql_dir, fname))
    with open(fp, 'r', encoding='utf-8') as f:
        return f.read()

def table_exists(cur, tab_name) -> bool:
    cur.execute(get_sql('table_exists.sql'), (tab_name,))
    return bool(cur.fetchone()[0])

def main(conn):
    """Main entrypoint function"""
    cur = conn.cursor()
    table = [['foo', 'bar'], 
             ['a', 1], 
             ['b', 2], 
             ['c', 2]]

    tab_exists = table_exists(cur, 'foobar')
    logging.debug(f'tab_exists: {tab_exists}')
    # create table if DNE
    # petl.appenddb(table, cur, tab_name)


def get_opts():
    """Get command line options from argparse"""
    p = argparse.ArgumentParser(
        description=("Pull historical quotes from Alpaca REST API to "
                     "PostgreSQL DB. Please see `pysopg2.connect` docs "
                     "for details."))
    p.add_argument('--dbname', type=str, required=True)
    p.add_argument('--user', type=str, required=True)
    p.add_argument('--password', type=str, required=True)
    p.add_argument('--host', type=str, required=True)
    p.add_argument('--port', type=int, required=True)
    return p.parse_args()


if __name__ == '__main__':
    opts = get_opts()
    main(conn=get_pg_conn(opts))
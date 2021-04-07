import argparse
import petl
import psycopg2
from pdb import set_trace as st


def get_pg_uri(user, password, host, port, dbname) -> str:
    """Returns PostgreSQL URI-formatted string."""
    return f'postgresql://{user}:{password}@{host}:{port}/{dbname}'


def get_pg_conn(opts):
    """Connect to remote DB using credentials passed in command line"""
    kw = {k: getattr(opts, k) for k in 
          ('dbname', 'user', 'password', 'host', 'port')}
    return psycopg2.connect(get_pg_uri(**kw))


def get_sql(fname, sql_dir=None) -> str:
    """Reads SQL query in file at `fname`."""
    # DEBUG
    fp = 'alpaca2pg/sql/test.sql'
    with open(fp, 'r', encoding='utf-8') as f:
        return f.read()


def main(conn):
    """Main entrypoint function"""
    table = (
        petl
        .fromdb(conn, get_sql('test.sql'))
    )
    print(table.look())


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
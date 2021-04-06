import argparse
import petl
import psycopg2


def get_pgconn(opts):
    """Connect to remote DB using credentials passed in command line"""
    kw = {k: getattr(opts, k) for k in 
          ('dbname', 'user', 'password', 'host', 'port')}
    return psycopg2.connect(**kw)


def main(opts):
    """Main entrypoint function"""
    pgconn = get_pgconn(opts)
    table = (
        petl
        # table = etl.fromdb(pgconn, 'SELECT * FROM example')
    )


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
    main(opts)
import pytest
from alpaca2pg.__main__ import main
from datetime import datetime as dt, date


def test_main(env_context):
    start_date = dt.strptime('2020-10-01', '%Y-%m-%d').date()
    end_date = dt.strptime('2021-01-01', '%Y-%m-%d').date()
    ticker = 'TSLA'
    result = main(ticker=ticker, start_date=start_date, end_date=end_date)


    
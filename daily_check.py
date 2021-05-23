import datetime
from datetime import datetime, timedelta, date
import os
from update_stock_data import stock_markets

import pandas as pd
from sqlalchemy import MetaData, Table, Column, Date, Float, String, create_engine

# parameters
db_dir = os.environ['FLASK_DB_PATH']
database_file = db_dir + 'stock-indices.db'


# stock_markets = {
#     '^nkx': 'nikkei225',
#     '^hsi': 'hangseng',
#     '^twse': 'taiwanstockexchange',
#     '^shc': 'shanghai',
#     '^snx': 'sensex',
#     '^kospi': 'korea',
#     '^dji': 'dowjones',
#     '^spx': 'sp500',
#     '^ndq': 'nasdaq',
#     '^tsx': 'tronto',
#     '^ukx': 'uk100',
#     '^dax': 'dax30',
# }
# end parameterk

def init_sqlalchemy():
    metadata = MetaData()
    Nikkei225 = Table('Nikkei225', metadata,
                      Column('date', Date, primary_key=True),
                      Column('open', Float),
                      Column('high', Float),
                      Column('low', Float),
                      Column('close', Float),
                      Column('volume', Float)
                      )

    HangSeng = Table('HangSeng', metadata,
                     Column('date', String(10), primary_key=True),
                     Column('open', Float),
                     Column('high', Float),
                     Column('low', Float),
                     Column('close', Float),
                     Column('volume', Float)
                     )

    TaiwanStockExchange = Table('TaiwanStockExchange', metadata,
                                Column('date', String(10), primary_key=True),
                                Column('open', Float),
                                Column('high', Float),
                                Column('low', Float),
                                Column('close', Float),
                                Column('volume', Float)
                                )

    ShangHai = Table('ShangHai', metadata,
                     Column('date', String(10), primary_key=True),
                     Column('open', Float),
                     Column('high', Float),
                     Column('low', Float),
                     Column('close', Float),
                     Column('volume', Float)
                     )

    SENSEX = Table('SENSEX', metadata,
                   Column('date', String(10), primary_key=True),
                   Column('open', Float),
                   Column('high', Float),
                   Column('low', Float),
                   Column('close', Float),
                   Column('volume', Float)
                   )

    KOREA = Table('KOREA', metadata,
                  Column('date', String(10), primary_key=True),
                  Column('open', Float),
                  Column('high', Float),
                  Column('low', Float),
                  Column('close', Float),
                  Column('volume', Float)
                  )

    DowJones = Table('DowJones', metadata,
                     Column('date', String(10), primary_key=True),
                     Column('open', Float),
                     Column('high', Float),
                     Column('low', Float),
                     Column('close', Float),
                     Column('volume', Float)
                     )

    SP500 = Table('SP500', metadata,
                  Column('date', String(10), primary_key=True),
                  Column('open', Float),
                  Column('high', Float),
                  Column('low', Float),
                  Column('close', Float),
                  Column('volume', Float)
                  )

    NASDAQ = Table('NASDAQ', metadata,
                   Column('date', String(10), primary_key=True),
                   Column('open', Float),
                   Column('high', Float),
                   Column('low', Float),
                   Column('close', Float),
                   Column('volume', Float)
                   )

    Tronto = Table('Tronto', metadata,
                   Column('date', String(10), primary_key=True),
                   Column('open', Float),
                   Column('high', Float),
                   Column('low', Float),
                   Column('close', Float),
                   Column('volume', Float)
                   )

    UK100 = Table('UK100', metadata,
                  Column('date', String(10), primary_key=True),
                  Column('open', Float),
                  Column('high', Float),
                  Column('low', Float),
                  Column('close', Float),
                  Column('volume', Float)
                  )

    DAX30 = Table('DAX30', metadata,
                  Column('date', String(10), primary_key=True),
                  Column('open', Float),
                  Column('high', Float),
                  Column('low', Float),
                  Column('close', Float),
                  Column('volume', Float)
                  )

    engine = create_engine('sqlite:////{}'.format(database_file))
    metadata.create_all(engine)
    connection = engine.connect()
    return engine, connection


def init_date_range():
    # adjust weekend. if today is Sunday, change latest day to Friday from Saturday (1 day before today).
    today = datetime.today()
    if today.isoweekday() == 7:
        weekend_adjustment_latestdate = -1
    elif today.isoweekday() == 1:
        weekend_adjustment_latestdate = -2
    else:
        weekend_adjustment_latestdate = 0
    latest_date = today.date() + timedelta(days=-1 + weekend_adjustment_latestdate)

    if latest_date.isoweekday() == 1:
        weekend_adjustment_basedate = -2
    else:
        weekend_adjustment_basedate = 0
    base_date = latest_date + timedelta(days=-1 + weekend_adjustment_basedate)

    return base_date, latest_date


def download_data(engine, base_date, latest_date):
    df = pd.DataFrame(index=[], columns=[])
    for market in stock_markets.values():
        query = 'SELECT date, close FROM {} where date between "{}" and "{}"'.format(market, base_date, latest_date)
        globals()[market] = pd.read_sql_query(query, engine)

        close_tmp = globals()[market][['date', 'close']]
        close_tmp = close_tmp.set_index('date')
        close_tmp = close_tmp.rename(columns={'close': market})

        df = pd.concat([df, close_tmp], axis=1)

    df = df.sort_index(ascending=False)

    return df


def calculate_daily_return(df, target_date, latest_date):
    base_value = df.iloc[1]
    latest_value = df.iloc[0]
    return_ratio = latest_value / base_value
    return_ratio = ((return_ratio - 1) * 100).round(2)
    return_ratio = return_ratio.sort_values(ascending=False)

    pd.options.display.precision = 2

    return return_ratio


def main():
    engine, connection = init_sqlalchemy()
    base_date, latest_date = init_date_range()
    df = download_data(engine, base_date, latest_date)
    return_ratio = calculate_daily_return(df, base_date, latest_date)

    output_json = return_ratio.to_json()
    return output_json, base_date, latest_date

if __name__ == '__main__':
    main()

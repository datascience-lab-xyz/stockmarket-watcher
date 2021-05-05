import datetime
import os
from update_stock_data import stock_markets

import pandas as pd
from sqlalchemy import MetaData, Table, Column, Float, String, create_engine

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
                      Column('date', String(10), primary_key=True),
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


def setup_dataframe(engine, df):
    for market in stock_markets.values():
        globals()[market] = pd.read_sql_query('SELECT date, close FROM {}'.format(market), engine)
        close_tmp = globals()[market][['date', 'close']]
        close_tmp = close_tmp.set_index('date')
        close_tmp = close_tmp.rename(columns={'close': market})

        if df.empty == True:
            df = close_tmp
        else:
            df = pd.concat([df, close_tmp], axis=1)

    # fill n/a data with the previous date
    df = df.sort_index(ascending=False)
    df = df.fillna(method='bfill', limit=10)

    return df

def analysis(df):
    # 基準日を決める
    start = '2020-04-01'
    end = df.index[0]

    # 基準日の値で基準化　
    for market in df.columns:
        start_value = df[market][start]
        df[market] = df[market] /start_value


def main():
    engine, connection = init_sqlalchemy()
    df = pd.DataFrame()
    df = setup_dataframe(engine, df)
    analysis(df)


if __name__ == '__main__':
    main()

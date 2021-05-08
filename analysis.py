import datetime
from datetime import datetime, timedelta
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


def create_blank_dataframe(start, end):
    start_date = datetime.strptime(start, "%Y-%m-%d")#convert to datetime type
    end_date = datetime.strptime(end, "%Y-%m-%d")#convert to datetime type

    date = start_date
    days_list = [start]

    for i in range((end_date - start_date).days):
        date = date + timedelta(days=1)
        days_list.append(datetime.strftime(date, '%Y-%m-%d'))
    df = pd.DataFrame(index=days_list, columns=[])#欠損の無い日付だけが入ったDataFrameを作成

    return df



def downlad_data(engine, df, start, end):
    for market in stock_markets.values():
        query = 'SELECT date, close FROM {} where date between "{}" and "{}"'.format(market, start, end)
        globals()[market] = pd.read_sql_query(query, engine)

        close_tmp = globals()[market][['date', 'close']]
        close_tmp = close_tmp.set_index('date')
        close_tmp = close_tmp.rename(columns={'close': market})

        df = pd.concat([df, close_tmp], axis=1)

    df = df.sort_index(ascending=False)

    return df

def data_preprocessing(df):
    df = df.fillna(method='bfill', limit=28)#欠損値を埋める
    missing = df.isnull().sum()

    return df

def analysis(df, start):
    # 基準日の値で基準化　
    for market in df.columns:
        start_value = df[market][start]
        df[market] = df[market] /start_value

    result = df[:1].T
    label_in_df = df[:1].index.values[0]
    label_new = 'value change (%)'
    result = result.rename(columns={label_in_df: label_new }).sort_values(label_new, ascending=False)

    result_json = result.to_json()

    return result_json


def main():
    start = "2021-04-01"
    end = "2021-04-28"
    engine, connection = init_sqlalchemy()
    df = create_blank_dataframe(start, end)
    df = downlad_data(engine, df, start, end)
    df = data_preprocessing(df)
    result_json = analysis(df, start)

    return result_json


if __name__ == '__main__':
    main()

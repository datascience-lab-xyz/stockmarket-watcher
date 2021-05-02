import pandas as pd
import pandas_datareader.data as pdr
import os
import sqlite3
from datetime import datetime, timedelta, date

# parameters
db_dir = os.environ['FLASK_DB_PATH']
database_file = db_dir + 'stock-indices.db'
stock_markets = {
    '^NKX':'Nikkei225',
    '^HSI':'HangSeng',
    '^TWSE':'TaiwanStockExchange',
    '^SHC':'ShangHai',
    '^SNX':'SENSEX',
    '^KOSPI':'KOREA',
    '^DJI':'DowJones',
    '^SPX':'SP500',
    '^NDQ':'NASDAQ',
    '^TSX':'Tronto',
    '^UKX':'UK100',
    '^DAX':'DAX30',
}
start_default = '2010-01-01'
end = date.today()
# end parameters


def get_latest_date_from_DB(database_file, market_ticker):
    table_name = stock_markets[market_ticker]
    connection = sqlite3.connect(database_file)
    sql_command = "SELECT date FROM {} ORDER BY date DESC;".format(table_name)

    try:
        df = pd.read_sql(sql_command, connection)
        latest_DB_date = df['date'][0]
        return latest_DB_date
    except:
        return None


def update_marketdata():
    database_file = db_dir + 'stock-indices.db'
    connection = sqlite3.connect(database_file)

    for market_ticker in stock_markets.keys():
        latest_DB_date = get_latest_date_from_DB(database_file, market_ticker)

        if latest_DB_date is None:
            start = start_default
        else:
            start = datetime.strptime(latest_DB_date, '%Y-%m-%d') + timedelta(days=1)

        market_data = pdr.DataReader(market_ticker, 'stooq', start, end)#download data via Pandas DataReader
        market_data.index = market_data.index.strftime('%Y-%m-%d')#convert data type of date to string

        table_name = stock_markets[market_ticker]
        try:
            market_data.to_sql(table_name, connection, if_exists='append')
        except KeyError as e:
            print('error')
            return e

    connection.commit()


def main():
    update_marketdata()


if __name__ =="__main__":
    main()

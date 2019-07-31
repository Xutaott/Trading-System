# coding=utf-8

'''
Update the original database and every business date, after 18:00
Also update the inter database for out sample backtest
'''
from sqlalchemy.orm import sessionmaker
import datetime
import pandas as pd
from sqlalchemy import create_engine
from database.database_sqlite import DatabaseTable
from database.database_sqlite import load_index_bar, load_index_technical, \
    load_stock_technical, load_stock_moneyflow, \
    load_stock_bar, load_stock_basic
from database.inter_database import drop_all, find_common, \
    load_stock_valid_inter, load_inter_technical_inter, \
    load_inter_moneyflow_inter, load_inter_index_inter, load_inter_bar_inter

engine_original = create_engine(
    'postgresql://chenxutao:@localhost/chinesestock_pg')
DatabaseTable.metadata.create_all(engine_original)
DatabaseTable.metadata.bind = engine_original
DBSession = sessionmaker(bind=engine_original)
session = DBSession()

# Get the start_date of update
sql_statement = "SELECT date FROM \"Indexs\" ORDER BY date DESC LIMIT 1"
result = engine_original.execute(sql_statement).fetchall()
last_end_date = result[0][0]
start_date = pd.to_datetime(last_end_date, yearfirst=True) \
             + datetime.timedelta(days=1)
start_date = start_date.strftime('%Y%m%d')

# Get the end_date of update
end_date = datetime.datetime.now().strftime('%Y%m%d')

# Update database

# Update the StockBasic, which stores all lively listed stocks
engine_original.execute('DELETE FROM \"StockBasic\"')
load_stock_basic(session)
load_stock_bar(session, start_date, end_date)
load_stock_moneyflow(session, start_date, end_date)
load_stock_technical(session, start_date, end_date)
load_index_bar(session, start_date, end_date)
load_index_technical(session, start_date, end_date)

# Find the unlisted stock and delete them
sql_statement = "SELECT ts_code FROM \"StockBasic\""
result = engine_original.execute(sql_statement).fetchall()
live_stocks = [a[0] for a in result]

sql_statement = "SELECT DISTINCT ts_code FROM \"Stocks\""
result = engine_original.execute(sql_statement).fetchall()
all_stocks = [a[0] for a in result]

delive_stocks = list(set(all_stocks) - set(live_stocks))
for stock in delive_stocks:
    engine_original.execute(
        'DELETE FROM \"Stocks\" WHERE ts_code=\'%s\'' % stock)
    engine_original.execute(
        'DELETE FROM \"Moneyflow\" WHERE ts_code=\'%s\'' % stock)
    engine_original.execute(
        'DELETE FROM \"Technical\" WHERE ts_code=\'%s\'' % stock)

# Update inter database for out sample backtest

engine_inter_os = create_engine(
    'postgresql://chenxutao:@localhost/chinesestock_pg_inter_os')
# Get the end_date of update, leave 2 years for out sample test
start_date_os = datetime.datetime.now() - datetime.timedelta(days=730)
start_date_os = start_date_os.strftime('%Y%m%d')
end_date_os = datetime.datetime.now().strftime('%Y%m%d')

co_list = find_common(engine_original, start_date_os, end_date_os)

# Drop all table first to avoid duplicate
drop_all(engine_inter_os)

# Loading inter database
load_stock_valid_inter(engine_original, engine_inter_os, start_date_os,
                       end_date_os, co_list)
load_inter_bar_inter(engine_original, engine_inter_os, start_date_os,
                     end_date_os,
                     co_list)
load_inter_moneyflow_inter(engine_original, engine_inter_os, start_date_os,
                           end_date_os, co_list)
load_inter_technical_inter(engine_original, engine_inter_os, start_date_os,
                           end_date_os, co_list)
load_inter_index_inter(engine_original, engine_inter_os, start_date_os,
                       end_date_os)

'''
Update the original database every business date, after 18:00
'''
from database.database_sqlite import *
import datetime
import pandas as pd

engine = create_engine('postgresql://chenxutao:@localhost/chinesestock_pg')
DatabaseTable.metadata.create_all(engine)
DatabaseTable.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

# Get the start_date of update
sql_statement = "SELECT date FROM \"Indexs\" ORDER BY date DESC LIMIT 1"
result = engine.execute(sql_statement).fetchall()
last_end_date = result[0][0]
start_date = pd.to_datetime(last_end_date, yearfirst=True) \
             + datetime.timedelta(days=1)
start_date = start_date.strftime('%Y%m%d')

# Get the end_date of update
end_date = datetime.datetime.now() - datetime.timedelta(days=1)
end_date = end_date.strftime('%Y%m%d')

# Update database

# Update the StockBasic, which stores all lively listed stocks
engine.execute('DELETE FROM \"StockBasic\"')
load_stock_basic(session)
load_stock_bar(session, START_DATE, END_DATE)
load_stock_moneyflow(session, START_DATE, END_DATE)
load_stock_technical(session, START_DATE, END_DATE)
load_index_bar(session, START_DATE, END_DATE)
load_index_technical(session, START_DATE, END_DATE)

# Find the unlisted stock and delete them
sql_statement = "SELECT ts_code FROM \"StockBasic\""
result = engine.execute(sql_statement).fetchall()
live_stocks = [a[0] for a in result]

sql_statement = "SELECT DISTINCT ts_code FROM \"Stocks\""
result = engine.execute(sql_statement).fetchall()
all_stocks = [a[0] for a in result]

delive_stocks = list(set(all_stocks) - set(live_stocks))
for stock in delive_stocks:
    engine.execute('DELETE FROM \"Stocks\" WHERE ts_code=\'%s\'' % stock)
    engine.execute('DELETE FROM \"Moneyflow\" WHERE ts_code=\'%s\'' % stock)
    engine.execute('DELETE FROM \"Technical\" WHERE ts_code=\'%s\'' % stock)

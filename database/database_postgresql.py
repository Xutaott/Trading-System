# coding=utf-8

'''
Local Postgresql Database
1. Download data from Tushare API:
  Stock:
    a. stock basic
    b. stock daily: bar/technical/moneyflow
  Index:
    a. index basic
    b. index daily: bar/technical
2. Store in psql database
'''

from database.database_sqlite import load_stock_basic, load_stock_bar, \
    load_stock_moneyflow, load_stock_technical, load_index_basic, \
    load_index_technical, load_index_bar, DatabaseTable
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

if __name__ == "__main__":
    print('Using PostgreSQL as database')

    START_DATE = '20100101'
    END_DATE = '20190430'

    engine = create_engine('postgresql://chenxutao:@localhost/chinesestock_pg')

    # DatabaseTable.metadata.drop_all(engine)  # Drop Table to restart
    DatabaseTable.metadata.create_all(engine)
    DatabaseTable.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    load_stock_basic(session)
    load_stock_bar(session, START_DATE, END_DATE)
    load_stock_moneyflow(session, START_DATE, END_DATE)
    load_stock_technical(session, START_DATE, END_DATE)
    load_index_basic(session)
    load_index_bar(session, START_DATE, END_DATE)
    load_index_technical(session, START_DATE, END_DATE)

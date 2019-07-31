# coding=utf-8

'''
At the beginning of months, update the inter database for in sample backtest
'''

from sqlalchemy import create_engine
from database.inter_database import find_common, drop_all, \
    load_inter_bar_inter, load_inter_index_inter, load_inter_moneyflow_inter, \
    load_inter_technical_inter, load_stock_valid_inter
import datetime

engine_original = create_engine(
    'postgresql://chenxutao:@localhost/chinesestock_pg')
engine_inter = create_engine(
    'postgresql://chenxutao:@localhost/chinesestock_pg_inter')

START_DATE = '20100101'

# Get the end_date of update, leave 2 years for out sample test
end_date = datetime.datetime.now() - datetime.timedelta(days=730)
end_date = end_date.strftime('%Y%m%d')

co_list = find_common(engine_original, START_DATE, end_date)

# Drop all table first to avoid duplicate
drop_all(engine_inter)
load_stock_valid_inter(engine_original, engine_inter, START_DATE, end_date,
                       co_list)
load_inter_bar_inter(engine_original, engine_inter, START_DATE, end_date,
                     co_list)
load_inter_moneyflow_inter(engine_original, engine_inter, START_DATE, end_date,
                           co_list)
load_inter_technical_inter(engine_original, engine_inter, START_DATE, end_date,
                           co_list)
load_inter_index_inter(engine_original, engine_inter, START_DATE, end_date)

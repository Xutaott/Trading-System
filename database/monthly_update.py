'''
Update the inter database at the first date of every month
'''

from database.inter_database import *
import datetime

engine_original = create_engine(
    'postgresql://chenxutao:@localhost/chinesestock_pg')
engine_inter = create_engine(
    'postgresql://chenxutao:@localhost/chinesestock_pg_inter')

# Get the end_date of update, leave 2 years for out sample test
end_date = datetime.datetime.now() - datetime.timedelta(days=730)
end_date = end_date.strftime('%Y%m%d')

co_list = find_common(engine_original, END_DATE)

# Drop all table first to avoid duplicate
drop_all(engine_inter)
load_stock_valid(engine_original, engine_inter, END_DATE, co_list)
load_inter_bar(engine_original, engine_inter, END_DATE, co_list)
load_inter_moneyflow(engine_original, engine_inter, END_DATE, co_list)
load_inter_technical(engine_original, engine_inter, END_DATE, co_list)
load_inter_index(engine_original, engine_inter, END_DATE)

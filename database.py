'''
Local Database
Download daily OHLC data from Tushare API and store in sqlite database
Xutao Chen
Mar. 9th
'''

# Import necessary package
from sqlalchemy import Column, String, Float, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import datetime
import tushare as ts
import json

# Connect tushare api with token
token_xutao = '9137df9ba18a9982171c1804ae874b7e41f6431cea3a752064cdec47'
ts.set_token(token_xutao)
pro = ts.pro_api()

# Initialize database
Base = declarative_base()


class Stock_basic(Base):
    __tablename__ = 'StockBasic'
    ts_code = Column(String(50), nullable=False, primary_key=True)
    symbol = Column(String(50), nullable=False)
    name = Column(String(50), nullable=False)
    area = Column(String(50), nullable=False)
    industry = Column(String(50), nullable=False)
    list_dt = Column(String(50), nullable=False)


class Stock(Base):
    __tablename__ = 'Stocks'
    ts_code = Column(String(50), nullable=False, primary_key=True)
    date = Column(String(50), nullable=False, primary_key=True)
    open_p = Column(Float, nullable=False)
    close_p = Column(Float, nullable=False)
    high_p = Column(Float, nullable=False)
    low_p = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    pre_close = Column(Float, nullable=True)


engine = create_engine('sqlite:///chinesestock.db')
# Base.metadata.drop_all(engine) # Drop Table to restart
Base.metadata.create_all(engine)
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

# Load Stock Basic Data
df_basic = pro.stock_basic(exchange='', list_status='L')
data_json = json.loads(df_basic.to_json(orient='table'))
data = data_json['data']
for record in data:
    ts_code = record['ts_code']
    symbol = record['symbol']
    name = record['name']
    area = record['area']
    industry = record['industry']
    list_dt = record['list_date']
    aStockBasic = Stock_basic(ts_code=ts_code, symbol=symbol, name=name,
                              area=area, industry=industry, list_dt=list_dt)
    session.add(aStockBasic)
session.commit()

# Load Stock Daily Data
start_dt = '20000101'
time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
end_dt = time_temp.strftime('%Y%m%d')

stock_pool = session.query(Stock_basic.ts_code).all()
stock_pool = list(a[0] for a in stock_pool)
total = len(stock_pool)

for i in range(total):
    try:
        df = pro.daily(ts_code=stock_pool[i], start_date=start_dt,
                       end_date=end_dt)
        print('Seq: ' + str(i + 1) + ' of ' + str(total) +
              '  Symbol: ' + stock_pool[i])
        date_len = df.shape[0]
    except Exception as ex:
        print(ex)
        print('No Data for ' + stock_pool[i])
        continue
    data_json = json.loads(df.to_json(orient='table'))
    data = data_json['data']
    for record in data:
        ts_code = record['ts_code']
        date = record['trade_date']
        open_p = record['open']
        close_p = record['close']
        high_p = record['high']
        low_p = record['low']
        volume = record['vol']
        amount = record['amount']
        pre_close = record['pre_close']
        aStock = Stock(ts_code=ts_code, date=date, open_p=open_p,
                       close_p=close_p,
                       high_p=high_p, low_p=low_p, volume=volume,
                       amount=amount,
                       pre_close=pre_close)
        session.add(aStock)
    session.commit()

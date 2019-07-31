# coding=utf-8

'''
Local Sqlite Database
1. Download data from Tushare API:
  Stock:
    a. stock basic
    b. stock daily: bar/technical/moneyflow
  Index:
    a. index basic
    b. index daily: bar/technical
2. Store in sqlite database

'''

# Import necessary package
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import tushare as ts
import json
import time

# Set up constant variable
# token for tushare API
TOKEN_XUTAO = '9137df9ba18a9982171c1804ae874b7e41f6431cea3a752064cdec47'
START_DATE = '20100101'
END_DATE = '20190430'
# '000001.SH':上证指数; '000300.SH':沪深300; '000905.SH': 中证500;
# '399001.SZ':深证成指; '399005.SZ':中小板指数; '399006.SZ':创业板指数;
# '399016.SZ':深证创新
INDEX_LIST = ['000001.SH', '000300.SH', '000905.SH', '399001.SZ',
              '399005.SZ', '399006.SZ', '399016.SZ']
# time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
# end_date = time_temp.strftime('%Y%m%d')


# Connect tushare api with token
ts.set_token(TOKEN_XUTAO)
tushare_online_data = ts.pro_api()

# Initialize database
DatabaseTable = declarative_base()


# table class for stock basic
class Stock_Basic(DatabaseTable):
    __tablename__ = 'StockBasic'
    ts_code = Column(String(50), nullable=False, primary_key=True)
    symbol = Column(String(50), nullable=False)
    name = Column(String(50), nullable=False)
    area = Column(String(50), nullable=False)
    industry = Column(String(50), nullable=False)
    list_dt = Column(String(50), nullable=False)


# table class for stocks bar data
class Stock(DatabaseTable):
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


# table class for stocks moneyflow data
class Moneyflow(DatabaseTable):
    __tablename__ = 'Moneyflow'
    # 小单：5万以下 中单：5万～20万 大单：20万～100万 特大单：成交额>=100万
    ts_code = Column(String(50), nullable=False, primary_key=True)
    date = Column(String(50), nullable=False, primary_key=True)
    buy_sm_vol = Column(Integer, nullable=True)
    buy_sm_amount = Column(Float, nullable=True)
    sell_sm_vol = Column(Integer, nullable=True)
    sell_sm_amount = Column(Float, nullable=True)
    buy_md_vol = Column(Integer, nullable=True)
    buy_md_amount = Column(Float, nullable=True)
    sell_md_vol = Column(Integer, nullable=True)
    sell_md_amount = Column(Float, nullable=True)
    buy_lg_vol = Column(Integer, nullable=True)
    buy_lg_amount = Column(Float, nullable=True)
    sell_lg_vol = Column(Integer, nullable=True)
    sell_lg_amount = Column(Float, nullable=True)
    buy_elg_vol = Column(Integer, nullable=True)
    buy_elg_amount = Column(Float, nullable=True)
    sell_elg_vol = Column(Integer, nullable=True)
    sell_elg_amount = Column(Float, nullable=True)
    net_mf_vol = Column(Integer, nullable=True)
    net_mf_amount = Column(Integer, nullable=True)


# table class for stocks technical data
class Technical(DatabaseTable):
    __tablename__ = 'Technical'
    ts_code = Column(String(50), nullable=False, primary_key=True)
    date = Column(String(50), nullable=False, primary_key=True)
    close_p = Column(Float, nullable=True)
    turnover_rate = Column(Float, nullable=True)
    turnover_rate_f = Column(Float, nullable=True)
    # 量比
    volume_ratio = Column(Float, nullable=True)
    # 市盈率
    pe_lyr = Column(Float, nullable=True)
    pe_ttm = Column(Float, nullable=True)
    # 市净率
    pb = Column(Float, nullable=True)
    # 市销率
    ps_lyr = Column(Float, nullable=True)
    ps_ttm = Column(Float, nullable=True)
    total_share = Column(Float, nullable=True)
    float_share = Column(Float, nullable=True)
    free_share = Column(Float, nullable=True)
    # 总市值与流通市值
    total_mv = Column(Float, nullable=True)
    float_mv = Column(Float, nullable=True)


# table class for index basic data
class Index_Basic(DatabaseTable):
    __tablename__ = 'IndexBasic'
    ts_code = Column(String(50), nullable=False, primary_key=True)
    name = Column(String(50), nullable=True)
    market = Column(String(50), nullable=True)
    publisher = Column(String(50), nullable=True)
    category = Column(String(50), nullable=True)
    base_date = Column(String(50), nullable=True)
    base_point = Column(Float, nullable=True)
    list_date = Column(String(50), nullable=True)


# table class for index bar data
class Index(DatabaseTable):
    __tablename__ = 'Indexs'
    ts_code = Column(String(50), nullable=False, primary_key=True)
    date = Column(String(50), nullable=False, primary_key=True)
    open_p = Column(Float, nullable=True)
    close_p = Column(Float, nullable=True)
    high_p = Column(Float, nullable=True)
    low_p = Column(Float, nullable=True)
    pre_close = Column(Float, nullable=True)
    pct_change = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    amount = Column(Float, nullable=True)


# table class for index technical data
class Technical_Index(DatabaseTable):
    __tablename__ = 'TechnicalIndex'
    ts_code = Column(String(50), nullable=False, primary_key=True)
    date = Column(String(50), nullable=False, primary_key=True)
    turnover_rate = Column(Float, nullable=True)
    turnover_rate_f = Column(Float, nullable=True)
    # 市盈率
    pe_lyr = Column(Float, nullable=True)
    pe_ttm = Column(Float, nullable=True)
    # 市净率
    pb = Column(Float, nullable=True)
    total_share = Column(Float, nullable=True)
    float_share = Column(Float, nullable=True)
    free_share = Column(Float, nullable=True)
    # 总市值与流通市值
    total_mv = Column(Float, nullable=True)
    float_mv = Column(Float, nullable=True)


# Load Stock Basic Data
def load_stock_basic(session):
    print("Loading stock basic")
    t1 = time.time()
    # Only select actively listed to date
    df_basic = tushare_online_data.stock_basic(exchange='', list_status='L')
    data_json = json.loads(df_basic.to_json(orient='table'))
    data = data_json['data']
    for record in data:
        # Some issues in stock 001872.SZ, drop it
        if record['ts_code'] == '001872.SZ':
            continue
        else:
            ts_code = record['ts_code']
            symbol = record['symbol']
            name = record['name']
            area = record['area']
            industry = record['industry']
            list_dt = record['list_date']
            stock_instance = Stock_Basic(ts_code=ts_code, symbol=symbol,
                                         name=name, industry=industry,
                                         list_dt=list_dt, area=area)
            session.add(stock_instance)
    session.commit()
    t2 = time.time()
    print(t2 - t1)


# Load stock bar data
def load_stock_bar(session, start_date, end_date):
    print("Loading stock bar")
    t1 = time.time()
    # Load list of actively listed stock
    stock_pool = session.query(Stock_Basic.ts_code).all()
    stock_pool = list(a[0] for a in stock_pool)
    stock_count = len(stock_pool)

    for stock_idx in range(stock_count):
        while True:  # While loop to ensure retrieval of a certain symbol
            try:
                df = tushare_online_data.daily(ts_code=stock_pool[stock_idx],
                                               start_date=start_date,
                                               end_date=end_date)
                print('Seq: ' + str(stock_idx + 1) + ' of ' + str(stock_count)
                      + '  Symbol: ' + stock_pool[stock_idx])
            except Exception as ex:
                print(ex)
                print('No Data for ' + stock_pool[stock_idx])
                continue
            break
        # Convert into json format
        data_json = json.loads(df.to_json(orient='table'))
        data = data_json['data']
        # Write into database
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

            aBar = Stock(ts_code=ts_code, date=date,
                         open_p=open_p, close_p=close_p,
                         high_p=high_p, low_p=low_p,
                         volume=volume, amount=amount,
                         pre_close=pre_close)
            session.add(aBar)
        session.commit()
    t2 = time.time()
    print(t2 - t1)


# Load stock moneyflow data
def load_stock_moneyflow(session, start_date, end_date):
    print("Loading stock moneyflow")
    t1 = time.time()
    stock_pool = session.query(Stock_Basic.ts_code).all()
    stock_pool = list(a[0] for a in stock_pool)
    stock_count = len(stock_pool)

    for stock_idx in range(stock_count):
        while True:
            try:
                df = tushare_online_data.moneyflow(
                    ts_code=stock_pool[stock_idx],
                    start_date=start_date,
                    end_date=end_date)
                print(
                    'Seq: ' + str(stock_idx + 1) + ' of ' + str(stock_count) +
                    '  Symbol: ' + stock_pool[stock_idx])
            except Exception as ex:
                print(ex)
                print('No Data for ' + stock_pool[stock_idx])
                continue
            break
        data_json = json.loads(df.to_json(orient='table'))
        data = data_json['data']
        for record in data:
            ts_code = record['ts_code']
            date = record['trade_date']
            buy_sm_vol = record['buy_sm_vol']
            buy_sm_amount = record['buy_sm_amount']
            sell_sm_vol = record['sell_sm_vol']
            sell_sm_amount = record['sell_sm_amount']
            buy_md_vol = record['buy_md_vol']
            buy_md_amount = record['buy_md_amount']
            sell_md_vol = record['sell_md_vol']
            sell_md_amount = record['sell_md_amount']
            buy_lg_vol = record['buy_lg_vol']
            buy_lg_amount = record['buy_lg_amount']
            sell_lg_vol = record['sell_lg_vol']
            sell_lg_amount = record['sell_lg_amount']
            buy_elg_vol = record['buy_elg_vol']
            buy_elg_amount = record['buy_elg_amount']
            sell_elg_vol = record['sell_elg_vol']
            sell_elg_amount = record['sell_elg_amount']
            net_mf_vol = record['net_mf_vol']
            net_mf_amount = record['net_mf_amount']

            aMoneyflow = Moneyflow(ts_code=ts_code, date=date,
                                   buy_sm_vol=buy_sm_vol,
                                   buy_sm_amount=buy_sm_amount,
                                   sell_sm_vol=sell_sm_vol,
                                   sell_sm_amount=sell_sm_amount,
                                   buy_md_vol=buy_md_vol,
                                   buy_md_amount=buy_md_amount,
                                   sell_md_vol=sell_md_vol,
                                   sell_md_amount=sell_md_amount,
                                   buy_lg_vol=buy_lg_vol,
                                   buy_lg_amount=buy_lg_amount,
                                   sell_lg_vol=sell_lg_vol,
                                   sell_lg_amount=sell_lg_amount,
                                   buy_elg_vol=buy_elg_vol,
                                   buy_elg_amount=buy_elg_amount,
                                   sell_elg_vol=sell_elg_vol,
                                   sell_elg_amount=sell_elg_amount,
                                   net_mf_vol=net_mf_vol,
                                   net_mf_amount=net_mf_amount)
            session.add(aMoneyflow)
        session.commit()
    t2 = time.time()
    print(t2 - t1)


# Load stock technical data
def load_stock_technical(session, start_date, end_date):
    print("Loading stock technical")
    t1 = time.time()
    stock_pool = session.query(Stock_Basic.ts_code).all()
    stock_pool = list(a[0] for a in stock_pool)
    stock_count = len(stock_pool)

    for stock_idx in range(stock_count):
        while True:
            try:
                df = tushare_online_data.daily_basic(
                    ts_code=stock_pool[stock_idx], start_date=start_date,
                    end_date=end_date)
                print('Seq: ' + str(stock_idx + 1) + ' of ' + str(stock_count)
                      + '  Symbol: ' + stock_pool[stock_idx])
            except Exception as ex:
                print(ex)
                print('No Data for ' + stock_pool[stock_idx])
                continue
            break
        data_json = json.loads(df.to_json(orient='table'))
        data = data_json['data']
        for record in data:
            ts_code = record['ts_code']
            date = record['trade_date']
            close_p = record['close']
            turnover_rate = record['turnover_rate']
            turnover_rate_f = record['turnover_rate_f']
            volume_ratio = record['volume_ratio']
            pe_lyr = record['pe']
            pe_ttm = record['pe_ttm']
            pb = record['pb']
            ps_lyr = record['ps']
            ps_ttm = record['ps_ttm']
            total_share = record['total_share']
            float_share = record['float_share']
            free_share = record['free_share']
            total_mv = record['total_mv']
            float_mv = record['circ_mv']

            aTechnical = Technical(ts_code=ts_code, date=date, close_p=close_p,
                                   turnover_rate=turnover_rate,
                                   turnover_rate_f=turnover_rate_f,
                                   volume_ratio=volume_ratio, pe_lyr=pe_lyr,
                                   pe_ttm=pe_ttm, pb=pb, ps_lyr=ps_lyr,
                                   ps_ttm=ps_ttm, total_share=total_share,
                                   float_share=float_share,
                                   free_share=free_share, total_mv=total_mv,
                                   float_mv=float_mv)
            session.add(aTechnical)
        session.commit()
    t2 = time.time()
    print(t2 - t1)


# Load index basic data
def load_index_basic(session):
    print("Loading index basic")
    t1 = time.time()
    # SSE:上交所; SZSW:深交所; CSI:中证指数
    market_list = ['SSE', 'SZSE', 'CSI']
    for market in market_list:
        df = tushare_online_data.index_basic(market=market)
        data_json = json.loads(df.to_json(orient='table'))
        data = data_json['data']
        for record in data:
            ts_code = record['ts_code']
            name = record['name']
            market = record['market']
            publisher = record['publisher']
            category = record['category']
            base_date = record['base_date']
            base_point = record['base_point']
            list_date = record['list_date']

            aIndexBasic = Index_Basic(ts_code=ts_code, name=name,
                                      market=market, publisher=publisher,
                                      category=category, base_date=base_date,
                                      base_point=base_point,
                                      list_date=list_date)
            session.add(aIndexBasic)
        session.commit()
    t2 = time.time()
    print(t2 - t1)


# Load index bar data
def load_index_bar(session, start_date, end_date):
    print("Loading index bar")
    t1 = time.time()
    index_pool = INDEX_LIST
    index_count = len(index_pool)

    for index_idx in range(index_count):
        while True:
            try:
                df = tushare_online_data.index_daily(
                    ts_code=index_pool[index_idx], start_date=start_date,
                    end_date=end_date)
                print('Seq: ' + str(index_idx + 1) + ' of ' + str(index_pool) +
                      '  Symbol: ' + index_pool[index_idx])
            except Exception as ex:
                print(ex)
                print('No Data for ' + index_pool[index_idx])
                continue
            break
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
            pct_change = record['pct_chg']

            aIndexBar = Index(ts_code=ts_code, date=date, open_p=open_p,
                              close_p=close_p, high_p=high_p, low_p=low_p,
                              volume=volume, amount=amount,
                              pre_close=pre_close, pct_change=pct_change)
            session.add(aIndexBar)
        session.commit()
    t2 = time.time()
    print(t2 - t1)


# Load index technical data
def load_index_technical(session, start_date, end_date):
    print("Loading index technical")
    t1 = time.time()
    index_pool = INDEX_LIST
    index_count = len(index_pool)

    for index_idx in range(index_count):
        while True:
            try:
                df = tushare_online_data.index_dailybasic(
                    ts_code=index_pool[index_idx], start_date=start_date,
                    end_date=end_date)
                print('Seq: ' + str(index_idx + 1) + ' of ' + str(index_pool) +
                      '  Symbol: ' + index_pool[index_idx])
            except Exception as ex:
                print(ex)
                print('No Data for ' + index_pool[index_idx])
                continue
            break
        data_json = json.loads(df.to_json(orient='table'))
        data = data_json['data']
        for record in data:
            ts_code = record['ts_code']
            date = record['trade_date']
            turnover_rate = record['turnover_rate']
            turnover_rate_f = record['turnover_rate_f']
            # 市盈率
            pe_lyr = record['pe']
            pe_ttm = record['pe_ttm']
            # 市净率
            pb = record['pb']
            total_share = record['total_share']
            float_share = record['float_share']
            free_share = record['free_share']
            # 总市值与流通市值
            total_mv = record['total_mv']
            float_mv = record['float_mv']

            aIndexTechnical = Technical_Index(ts_code=ts_code, date=date,
                                              turnover_rate=turnover_rate,
                                              turnover_rate_f=turnover_rate_f,
                                              pe_lyr=pe_lyr, pe_ttm=pe_ttm,
                                              pb=pb,
                                              total_share=total_share,
                                              float_share=float_share,
                                              free_share=free_share,
                                              total_mv=total_mv,
                                              float_mv=float_mv)
            session.add(aIndexTechnical)
        session.commit()
    t2 = time.time()
    print(t2 - t1)


if __name__ == "__main__":
    print('Using SQLite as database')

    engine = create_engine('sqlite:///chinesestock.db')
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

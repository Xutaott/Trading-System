# coding=utf-8

# integrate all steps for backtest


from sqlalchemy import create_engine
import queue
import pandas as pd

from wts.datahandler import DailyDataHandler
from wts.event import NextLoopEvent
from wts.portfoliohandler import SimPortfolioHandler
from wts.executionhandler import SimExecutionHandler
from wts.performance import backtest_metric

# Set up the backtest parameter

# start and end of backtest period
START_DATE = "20100101"
END_DATE = "20170101"

# set decay+lookback<DIDX_START to ensure all alpha's orders
# to have same rebalance date
DIDX_START = 100

# rebalance frequency, capital, leverage ratio, allocation method
FREQ = 20
CAP = 10000000.0
LEVERAGE = 0.8
ALLO = 'AD'

# transaction spread and fees
SPREAD = 0.001
FEE = 0.0005

# Population postgresql
path_stock_inter = "postgresql://chenxutao:@localhost/chinesestock_pg_inter"
engine_stock = create_engine(path_stock_inter)

events_queue = queue.Queue()
datahandler = DailyDataHandler(engine_stock, START_DATE, END_DATE)


def run(strategy):
    portfoliohandler = SimPortfolioHandler(events_queue, freq=FREQ,
                                           initial_cap=CAP,
                                           leverage=LEVERAGE, allo=ALLO)
    executionHandler = SimExecutionHandler(events_queue, datahandler,
                                           spread=SPREAD, fee=FEE)

    nextloop_event = NextLoopEvent(didx=DIDX_START)
    events_queue.put(nextloop_event)

    while not events_queue.empty():
        event = events_queue.get()
        if event.type == "NextLoopEvent":
            strategy.update_signal(event)

        elif event.type == "SignalEvent":
            portfoliohandler.update_order(event)

        elif event.type == "OrderEvent":
            executionHandler.execute_order(event)

        elif event.type == "FillEvent" or event.type == "NotFillEvent":
            portfoliohandler.update_fill(event)

    all_holding, all_position, all_order = portfoliohandler.to_dataframe()
    date = pd.DataFrame(strategy.date, columns=['date'])
    all_holding = all_holding.merge(date, how='left', left_index=True,
                                    right_index=True)
    all_position = all_position.merge(date, how='left', left_index=True,
                                      right_index=True)
    all_order = all_order.merge(date, how='left', left_index=True,
                                right_index=True)

    all_holding.set_index(keys='date', drop=True, inplace=True)
    all_holding.index = pd.to_datetime(all_holding.index)

    all_position.set_index(keys='date', drop=True, inplace=True)
    all_position.index = pd.to_datetime(all_position.index)

    all_order.set_index(keys='date', drop=True, inplace=True)
    all_order.index = pd.to_datetime(all_order.index)

    all_holding.to_csv('../backtest_output/all_holding.csv')
    all_position.to_csv('../backtest_output/all_position.csv')
    all_order.to_csv('../backtest_output/all_order.csv')

    # Show the performance metrics
    backtest_metric()

    print('Backtest Finish!')

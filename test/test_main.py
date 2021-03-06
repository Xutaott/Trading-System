# coding=utf-8

import unittest
from sqlalchemy import create_engine
import queue
import numpy as np
import pandas as pd

from wts.datahandler import DailyDataHandler
from wts.event import NextLoopEvent
from wts.strategy import AlphaBase
from wts.portfoliohandler import SimPortfolioHandler
from wts.executionhandler import SimExecutionHandler

'''
Guideline for backtesting
1. Specify the backtesting time range
1. Define a new Alpha class, inherited from AlphaBase
    1.a Override __init__, specify the rolling window, number of long stocks,
        data you will use
    1.b Override alpha_generator, specify the calculation process
2. If needed, change the init capital, leverage, spread, fee,
   rebalance frequency and allocation method
'''
# Population postgresql
path_stock_inter = "postgresql://chenxutao:@localhost/chinesestock_pg_inter"
engine_stock = create_engine(path_stock_inter)

start_dt = "20100101"
end_dt = "20170101"


class Alpha1(AlphaBase):
    def __init__(self, datahandler, events_queue):
        super(Alpha1, self).__init__(datahandler, events_queue)

        self.lookback = 10
        self.N = 50
        self.buy_sm_vol = self.get_data("buy_sm_amount")

    def alpha_generator(self, didx):
        num_Insts = len(self.symbol)
        alpha = np.array([np.nan] * num_Insts)
        v = [True] * num_Insts
        for i in range(1, self.lookback + 1):
            v1 = self.valid[didx - i, :]
            v = np.logical_and(v, v1)
        startdidx = didx - self.lookback

        # Pay attention to "axis" parameter
        buy_sm_vol = self.buy_sm_vol[startdidx:didx, v]
        alpha[v] = np.nanmean(buy_sm_vol, axis=0) / np.nanstd(buy_sm_vol,
                                                              axis=0)
        return alpha


class TestMain(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.events_queue = queue.Queue()
        self.datahandler = DailyDataHandler(engine_stock, start_dt, end_dt)
        self.strategy = Alpha1(self.datahandler, self.events_queue)
        self.portfoliohandler = SimPortfolioHandler(self.events_queue, freq=20,
                                                    initial_cap=10000000.0,
                                                    leverage=0.8,
                                                    allo='AD')
        self.executionHandler = SimExecutionHandler(self.events_queue,
                                                    self.datahandler,
                                                    spread=0.001,
                                                    fee=0.0005)

    def tearDown(self):
        self.assertTrue(True)

    def testCase1(self):

        # Arbitrary didx to start
        nextloop_event = NextLoopEvent(didx=20)
        self.events_queue.put(nextloop_event)

        while not self.events_queue.empty():
            event = self.events_queue.get()
            if event.type == "NextLoopEvent":
                self.strategy.update_signal(event)

            elif event.type == "SignalEvent":
                self.portfoliohandler.update_order(event)

            elif event.type == "OrderEvent":
                self.executionHandler.execute_order(event)

            elif event.type == "FillEvent" or event.type == "NotFillEvent":
                self.portfoliohandler.update_fill(event)

        all_holding, all_position = self.portfoliohandler.to_dataframe()
        date = pd.DataFrame(self.strategy.date, columns=['date'])
        all_holding = all_holding.merge(date, how='left', left_index=True,
                                        right_index=True)
        all_position = all_position.merge(date, how='left', left_index=True,
                                          right_index=True)

        all_holding.set_index(keys='date', drop=True, inplace=True)
        all_holding.index = pd.to_datetime(all_holding.index)

        all_position.set_index(keys='date', drop=True, inplace=True)
        all_position.index = pd.to_datetime(all_position.index)

        all_holding.to_csv("../backtest_output/all_holding.csv")
        all_position.to_csv("../backtest_output/all_position.csv")

        print("Backtest Finish!")

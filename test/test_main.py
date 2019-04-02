# coding=utf-8

import unittest
from sqlalchemy import create_engine
import queue
import pandas as pd

from wts.datahandler import DailyDataHandler
from wts.event import NextLoopEvent
from wts.strategy import AlphaBase
from wts.portfoliohandler import SimPortfolioHandler
from wts.executionhandler import SimExecutionHandler

start_datetime = "20100101"
end_datetime = "20190101"
# Sample database
# engine_stock = create_engine("sqlite:///../database/sample.db")
# Population database
engine_stock = create_engine("sqlite://///Users/chenxutao/Documents/"
                             "TradingSystem/chinesestock1.db")


# engine = create_engine("sqlite:///../database/chinesestock1.db")


class TestMain(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)

    def tearDown(self):
        self.assertTrue(True)

    def testCase1(self):
        events_queue = queue.Queue()
        datahandler = DailyDataHandler(engine_stock, start_datetime,
                                       end_datetime)
        strategy = AlphaBase(datahandler, events_queue)
        portfoliohandler = SimPortfolioHandler(events_queue, freq=20,
                                               initial_cap=1000000.0)
        executionHandler = SimExecutionHandler(events_queue, datahandler)
        # Arbitrage didx to start
        nextloop_event = NextLoopEvent(didx=4)
        events_queue.put(nextloop_event)

        while not events_queue.empty():
            event = events_queue.get()
            if event.type == "NextLoopEvent":
                strategy.update_signal(event)

            elif event.type == "SignalEvent":
                portfoliohandler.update_order(event, method="AD")

            elif event.type == "OrderEvent":
                executionHandler.execute_order(event)

            elif event.type == "FillEvent" or event.type == "NotFillEvent":
                portfoliohandler.update_fill(event)

        all_holding, all_position = portfoliohandler.to_dataframe()

        all_holding.to_csv("../backtest_output/all_holding.csv")
        all_position.to_csv("../backtest_output/all_position.csv")

        print("Backtest Finish!")

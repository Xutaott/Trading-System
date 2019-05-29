import unittest
import queue
import pandas as pd

from wts.position import Position
from wts.holding import Holding
from wts.portfoliohandler import SimPortfolioHandler
from wts.event import SignalEvent, FillEvent, NotFillEvent

events_queue = queue.Queue()




class TestPortfolioHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        first_position_date = pd.to_datetime("2010-01-07")
        self.portfolio_handler = SimPortfolioHandler(events_queue,
                                                     first_position_date,
                                                     initial_cap=1000000.0)

    def tearDown(self):
        self.assertTrue(True)

    # Test initial position and holding
    def testCase1(self):
        pass


'''

    # Test backtest end case
    def testCase2(self):
        nextloop_event = NextLoopEvent(pd.to_datetime("2010-12-30"))
        self.data_handler.stream_next(nextloop_event)
        self.assertEqual(events_queue.empty(), True)

    # Test Initial start case
    def testCase3(self):
        start_date = pd.to_datetime("2010-01-01")
        position_date = self.data_handler.find_trade_date(
            start_date, -self.data_handler.lookback)
        nextloop_event = NextLoopEvent(position_date)
        self.data_handler.stream_next(nextloop_event)
        data_event = events_queue.get()
        standard_output = pd.to_datetime("2010-01-11")
        self.assertEqual(data_event.datetime, standard_output)
        # print(data_event.batch)
'''

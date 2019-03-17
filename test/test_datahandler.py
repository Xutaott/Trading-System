import unittest
import queue
import pandas as pd

from wts.datahandler import DailyDataHandler
from sqlalchemy import create_engine
from wts.event import NextLoopEvent

engine = create_engine("sqlite:///../database/sample.db")
events_queue = queue.Queue()


class TestDataHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.data_handler = DailyDataHandler('2010-01-01', '2011-01-01',
                                             events_queue, engine,
                                             lookback=3, frequency=2)

    def tearDown(self):
        self.assertTrue(True)

    # Test find_trade_date
    def testCase1(self):
        date1 = pd.to_datetime('2010-12-31')
        self.assertEqual(self.data_handler.find_trade_date(date1, -1), False)

        date2 = pd.to_datetime("2010-12-30")
        self.assertEqual(self.data_handler.find_trade_date(date2, -5), False)

        date3 = pd.to_datetime("2010-01-04")
        self.assertEqual(self.data_handler.find_trade_date(date3, 1), False)

        date4 = pd.to_datetime("2010-01-01")
        self.assertEqual(self.data_handler.find_trade_date(date3, 1), False)

        # date5 = pd.to_datetime('2011-02-01')
        # System exit
        # self.data_handler.find_trade_date(date5, 1)

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

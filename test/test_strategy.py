import unittest
import queue
import pandas as pd
from wts.strategy import DailyStrategy
from wts.event import DataEvent

df = pd.read_csv("../test/test_sample.csv")
df["date"] = pd.to_datetime(df["date"])
events_queue = queue.Queue()


class TestStrategy(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.strategy = DailyStrategy(events_queue, lookback=5)

    def tearDown(self):
        self.assertTrue(True)

    def testCase1(self):
        lookback_start = pd.to_datetime("2010-01-04")
        lookback_end = pd.to_datetime("2010-01-08")
        next_order_date = pd.to_datetime("2010-01-11")
        batch = df[((df["date"] >= lookback_start) &
                    (df["date"] <= lookback_end))]
        data_event = DataEvent(next_order_date, batch)
        self.strategy.update_signal(data_event)
        signal_event = events_queue.get()
        self.assertEqual(signal_event.datetime, next_order_date)
        # print(signal_event.signal)

    def testCase2(self):
        lookback_start = pd.to_datetime("2010-06-29")
        lookback_end = pd.to_datetime("2010-07-05")
        next_order_date = pd.to_datetime("2010-07-06")
        batch = df[((df["date"] >= lookback_start) &
                    (df["date"] <= lookback_end))]
        data_event = DataEvent(next_order_date, batch)
        self.strategy.update_signal(data_event)
        signal_event = events_queue.get()
        self.assertEqual(signal_event.datetime, next_order_date)
        # print(signal_event.signal)

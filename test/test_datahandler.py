import unittest
import queue
from wts.datahandler import DailyDataHandler
from sqlalchemy import create_engine

engine = create_engine("sqlite:///../database/sample_stock.db")
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
        self.data_handler = DailyDataHandler(engine, "20100627", "20100703")

    def tearDown(self):
        self.assertTrue(True)

    # Test get_available function
    def testCase1(self):
        symbol, date, valid = self.data_handler.get_available()
        print(symbol)
        print(date)
        print(valid)

    # Test get_data function
    def testCase2(self):
        close = self.data_handler.get_data("close")
        buy_lg_vol = self.data_handler.get_data("buy_lg_vol")
        print(close)
        print(buy_lg_vol)

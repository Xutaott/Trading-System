import unittest
import queue
import pandas as pd
from sqlalchemy import create_engine
from wts.datahandler import DailyDataHandler
from wts.executionhandler import SimExecutionHandler
from wts.event import OrderEvent, NotFillEvent, FillEvent

engine = create_engine("sqlite:///../database/sample_stock.db")
events_queue = queue.Queue()
datahandler = DailyDataHandler(engine, "20100627", "20100703")


class TestExecutionHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.execution_handler = SimExecutionHandler(events_queue, datahandler)

    def tearDown(self):
        self.assertTrue(True)

    # Test not fill scenario
    def testCase1(self):
        didx = 2  # "20100630" in this case
        symbol = "000001.SZ"
        quantity = 200
        side = "Sell"
        order_type = "Market"
        price_range = (1, 1)  # Only for test
        order_event = OrderEvent(didx, symbol, quantity, side,
                                 price_range, order_type=order_type)
        self.execution_handler.execute_order(order_event)
        notfill_event = NotFillEvent(didx)
        event = events_queue.get()
        self.assertEqual(event.didx, notfill_event.didx)
        self.assertEqual(event.type, notfill_event.type)

    # Test fill scenario
    def testCase2(self):
        didx = 1  # "20100629"
        symbol = "000001.SZ"
        quantity = 200
        side = "Sell"
        order_type = "Market"
        order_event = OrderEvent(didx, symbol, quantity, side, order_type)
        self.execution_handler.execute_order(order_event)

        close_p = 17.51
        fee = close_p * quantity * 0.001
        fill_event = FillEvent(didx, symbol, quantity, close_p, side, fee)
        event = events_queue.get()
        self.assertEqual(event.didx, fill_event.didx)
        self.assertEqual(event.type, fill_event.type)
        self.assertEqual(event.symbol, fill_event.symbol)
        self.assertEqual(event.quantity, fill_event.quantity)
        self.assertEqual(event.price, fill_event.price)
        self.assertEqual(event.side, fill_event.side)
        self.assertEqual(event.fee, fill_event.fee)

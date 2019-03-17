import unittest
import queue
import pandas as pd
from sqlalchemy import create_engine

from wts.executionhandler import SimExecutionHandler
from wts.event import OrderEvent, NotFillEvent, FillEvent

engine = create_engine("sqlite:///../database/sample.db")
events_queue = queue.Queue()


class TestExecitionHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.execution_handler = SimExecutionHandler(events_queue, engine)

    def tearDown(self):
        self.assertTrue(True)

    # Test not fill scenario
    def testCase1(self):
        date = pd.to_datetime("2010-07-01")
        symbol = "000001.SZ"
        quantity = 200
        side = "Sell"
        order_type = "Market"
        order_event = OrderEvent(date, symbol, quantity, side, order_type)
        self.execution_handler.execute_order(order_event)
        notfill_event = NotFillEvent(date)
        event = events_queue.get()
        self.assertEqual(event.datetime, notfill_event.datetime)
        self.assertEqual(event.type, notfill_event.type)

    # Test fill scenario
    def testCase2(self):
        date = pd.to_datetime("2010-07-01")
        symbol = "000002.SZ"
        quantity = 200
        side = "Sell"
        order_type = "Market"
        order_event = OrderEvent(date, symbol, quantity, side, order_type)
        self.execution_handler.execute_order(order_event)

        close_p = 6.68
        fee = close_p * quantity * 0.001
        fill_event = FillEvent(date, symbol, quantity, close_p, side, fee)
        event = events_queue.get()
        self.assertEqual(event.datetime, fill_event.datetime)
        self.assertEqual(event.type, fill_event.type)
        self.assertEqual(event.symbol, fill_event.symbol)
        self.assertEqual(event.quantity, fill_event.quantity)
        self.assertEqual(event.price, fill_event.price)
        self.assertEqual(event.side, fill_event.side)
        self.assertEqual(event.fee, fill_event.fee)

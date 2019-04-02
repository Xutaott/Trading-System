import unittest
import queue
from sqlalchemy import create_engine
from wts.strategy import AlphaBase
from wts.datahandler import DailyDataHandler
from wts.event import NextLoopEvent


engine = create_engine("sqlite:///../database/sample_stock.db")
events_queue = queue.Queue()
datahandler = DailyDataHandler(engine, "20100627", "20100703")


class TestStrategy(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.strategy = AlphaBase(datahandler, events_queue, 3)

    def tearDown(self):
        self.assertTrue(True)

    # Test alpha_generator
    def testCase1(self):
        alpha = self.strategy.alpha_generator(didx=3)
        print(alpha)

    # Test signal_generator
    def testCase2(self):
        didx = 3
        alpha = self.strategy.alpha_generator(didx)
        signal = self.strategy.signal_generator(alpha, didx, N=2)
        print(signal)

    # Test update_signal
    def testCase3(self):
        nextloop_event = NextLoopEvent(3)
        self.strategy.update_signal(nextloop_event)
        event = self.strategy.events_queue.get()
        print(event.signal)
        print(event.didx)

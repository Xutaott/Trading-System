# coding=utf-8

import unittest
import queue
import numpy as np
from sqlalchemy import create_engine
from wts.strategy import AlphaBase
from wts.datahandler import DailyDataHandler
from wts.event import NextLoopEvent

path_stock_inter = "postgresql://chenxutao:@localhost/chinesestock_pg_inter"
engine_stock = create_engine(path_stock_inter)
events_queue = queue.Queue()
datahandler = DailyDataHandler(engine_stock, "20100627", "20100703")


class Alpha1(AlphaBase):
    def __init__(self, datahandler, events_queue):
        super(Alpha1, self).__init__(datahandler, events_queue)

        self.lookback = 3
        self.N = 50
        self.close = self.get_data("close_p")

    def alpha_generator(self, didx):
        num_Insts = len(self.symbol)
        alpha = np.array([np.nan] * num_Insts)
        v = [True] * num_Insts
        for i in range(1, self.lookback + 1):
            v1 = self.valid[didx - i, :]
            v = np.logical_and(v, v1)
        startdidx = didx - self.lookback

        # Pay attention to "axis" parameter
        buy_sm_vol = self.close[startdidx:didx, v]
        alpha[v] = np.nanmean(buy_sm_vol, axis=0)
        return alpha


class TestStrategy(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.strategy = Alpha1(datahandler, events_queue)

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
        signal = self.strategy.signal_generator(alpha, didx)
        print(signal)

    # Test update_signal
    def testCase3(self):
        nextloop_event = NextLoopEvent(3)
        self.strategy.update_signal(nextloop_event)
        event = self.strategy.events_queue.get()
        print(event.signal)
        print(event.didx)

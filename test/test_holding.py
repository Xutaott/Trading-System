# coding=utf-8

import unittest
import pandas as pd

from wts.holding import Holding
import copy

initial_cap = 1000000.0


class TestHolding(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.initial_holding = Holding(cash=initial_cap)
        self.all_holding = dict()

    def tearDown(self):
        self.assertTrue(True)

    def testCase1(self):
        # test initial case
        df = self.initial_holding.to_dataframe()
        print(df)
        current_date = pd.to_datetime("2010-01-07")
        self.all_holding[current_date] = copy.deepcopy(self.initial_holding)

        # test update_holding function
        delta_cash = -4520
        delta_stock = 4500
        fee = 4.52

        self.initial_holding.update_holding(delta_cash, delta_stock, fee)
        df = self.initial_holding.to_dataframe()
        print(df)
        current_date = pd.to_datetime("2010-01-08")
        self.all_holding[current_date] = copy.deepcopy(self.initial_holding)

        # Test output to dataframe
        all_holding = []
        for date, holding in self.all_holding.items():
            df = holding.to_dataframe()
            df["datetime"] = date
            all_holding.append(df)
        all_holding = pd.concat(all_holding)
        all_holding.set_index("datetime", inplace=True)
        print(all_holding)

import unittest
import pandas as pd

from wts.position import Position
from wts.event import FillEvent
import copy


class TestPosition(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        assert (True)

    @classmethod
    def tearDownClass(cls):
        assert (True)

    def setUp(self):
        self.assertTrue(True)
        self.initial_position = Position()
        self.all_position = dict()

    def tearDown(self):
        self.assertTrue(True)

    def testCase1(self):
        # test initial case and get_position function
        self.initial_position.get_position("000001.SZ")
        df = self.initial_position.to_dataframe()
        print(df)
        current_date = pd.to_datetime("2010-01-07")
        self.all_position[current_date] = copy.deepcopy(self.initial_position)

        # test update_position function
        date = pd.to_datetime("2010-01-08")
        symbol = "000001.SZ"
        quantity = 200
        side = "Buy"
        close_p = 22.6
        fee = close_p * quantity * 0.001

        fill_event = FillEvent(date, symbol, quantity, close_p, side, fee)
        self.initial_position.update_position(fill_event)
        df = self.initial_position.to_dataframe()
        print(df)
        current_date = date
        self.all_position[current_date] = copy.deepcopy(self.initial_position)

        # Test output to dataframe
        all_position = []
        for date, postion in self.all_position.items():
            df = postion.to_dataframe()
            df["datetime"] = date
            all_position.append(df)
        all_position = pd.concat(all_position)
        all_position.set_index("datetime", inplace=True)
        print(all_position)

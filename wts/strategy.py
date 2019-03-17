from abc import ABCMeta, abstractmethod
from wts.datahandler import DailyDataHandler
from wts.event import SignalEvent, NextLoopEvent
import pandas as pd
import numpy as np


class Strategy():
    '''
        Strategy is a base class providing an interface for all inherited
        strategy to generate trading signals
    '''
    __metaclass__ = ABCMeta

    @abstractmethod
    def backtest_sim(self, start_date, end_date, engine):
        '''
        Initialize the backtest simulation
        :param start_date: String
        :param end_date: String
        :param engine: Engine, bind to the stock database
        '''
        raise NotImplementedError("Should Implement backtest_sim method")

    @abstractmethod
    def update_signal(self, data_event):
        '''
        Update the signals when new data comes in
        :param data_event: DataEvent
        '''
        raise NotImplementedError("Should Implement update_signal method")

    @abstractmethod
    def signals_generator(self, batch):
        '''
        Implement your alpha strategy
        :param batch: Dataframe
        :return Dict, {String symbol:String side}
        '''
        raise NotImplementedError("Should Implement update_signal method")


class DailyStrategy(Strategy):
    '''
    Strategy that takes OHLCV data as input to generate signals
    '''

    def __init__(self, events_queue, lookback=1, frequency=1):
        '''

        :param lookback: Int, days of rolling window
        :param frequency: Int, frequency (in days) to rebalance
        :param events_queue: Queue
        '''
        self.lookback = lookback
        self.frequency = frequency
        self.events_queue = events_queue

    def update_signal(self, data_event):
        date = data_event.datetime
        batch = data_event.batch

        signal = self.signals_generator(batch)
        signal_event = SignalEvent(date, signal)
        self.events_queue.put(signal_event)

    def signals_generator(self, batch):
        '''
        Key method. Implement the strategy
        TODO: Generalize a calculation method, it could take a alpha
        TODO: equation and long the top 5% automatically
        :param batch: Dataframe
        :return: Dict, {String symbol: String side}
        '''
        # For testing, assume long the first 4 stock
        symbol_list = np.sort(batch["ts_code"].unique())
        target_symbol = symbol_list[:4]
        signal = dict()
        for symbol in target_symbol:
            signal[symbol] = "Buy"
        return signal

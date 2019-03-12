from abc import ABCMeta, abstractmethod
from datahandler import DailyDataHandler
from event import SignalEvent


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
        '''
        raise NotImplementedError("Should Implement update_signal method")


class DailyStrategy(Strategy):
    '''
    Strategy that takes OHLCV data as input to generate signals
    '''

    def __init__(self, lookback, frequency, events_queue):
        '''

        :param lookback: Int, days of rolling window
        :param frequency: Int, frequency (in days) to rebalance
        :param events_queue: Queue
        '''
        self.lookback = lookback
        self.frequency = frequency
        self.events_queue = events_queue

    def backtest_sim(self, start_date, end_date, engine):
        self.datahandler = DailyDataHandler(start_date, end_date,
                                            self.events_queue, engine)
        # Initialize backtest simulation
        pass

    def update_signal(self, data_event):
        date = data_event.datetime
        batch = data_event.batch
        signal = self.signals_generator(batch)
        signal_event = SignalEvent(date, signal)
        self.events_queue.put(signal_event)

    def signals_generator(self, batch):
        pass

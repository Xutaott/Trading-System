from abc import ABCMeta, abstractmethod
import pandas as pd
from wts.event import DataEvent
import numpy as np
import sys


class DataHandler(object):
    '''
    DataHandler is a base class providing an interface for all inherited
    data handler to stream data to strategy in time sequence
    '''
    __metaclass__ = ABCMeta

    @abstractmethod
    def stream_next(self, executed_event):
        '''
        After all orders of last period are executed, generate a DataEvent
        instance and put it to queue
        :param executed_event: ExecutedEvent
        '''
        raise NotImplementedError("Should Implement stream_next method")


class DailyDataHandler(DataHandler):
    '''
    Stream daily OHLCV data (open,high,low,close,volume) for backtest
    It's specific for each strategy
    '''

    def __init__(self, start_date, end_date, events_queue, engine,
                 lookback=1, frequency=1):
        '''

        :param start_date: String, start date of backtest period, "2018-01-01"
        :param end_date: String, end date of backtest period
        :param events_queue: Queue
        :param engine: EngineConnect, engine bind to the database
        :param lookback: Int, lookback period for this strategy
        :param frequency: Int, frequency of data stream
        '''
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.events_queue = events_queue
        self.engine = engine
        self.lookback = lookback
        self.frequency = frequency
        self.stocks, self.stock_basic = self._load_sql()
        self.all_date = pd.Series(
            np.flip(np.sort(self.stocks["date"].unique()), axis=0))
        self.all_date.sort_values()
        index = self.all_date.index
        self.first_date = self.all_date[index[-1]]
        self.last_date = self.all_date[index[0]]

    def _load_sql(self):
        '''
        Load data from stock database
        :return: dataframe
        '''
        # engine = create_engine('sqlite:///sample.db')
        sql_statement = "SELECT * FROM Stocks"
        result = self.engine.execute(sql_statement)
        keys = result.keys()
        df = [data for data in result]
        df = pd.DataFrame(df, columns=keys)
        df["date"] = pd.to_datetime(df["date"])
        df = df[((df["date"] >= self.start_date) &
                 (df["date"] <= self.end_date))]

        sql_statement = "SELECT * FROM StockBasic"
        result = self.engine.execute(sql_statement)
        keys = result.keys()
        df2 = [data for data in result]
        df2 = pd.DataFrame(df2, columns=keys)
        return df, df2

    def stream_next(self, nextloop_event):
        '''
        :param nextloop_event: NextLoopEvent
        '''
        last_order_date = nextloop_event.datetime
        next_order_date = self.find_trade_date(last_order_date,
                                               -self.frequency)
        if not next_order_date:
            # No new event in the queue, backtest terminate
            pass

        else:
            lookback_end = self.find_trade_date(next_order_date, 1)
            lookback_start = self.find_trade_date(lookback_end,
                                                  self.lookback - 1)
            batch = self.stocks[((self.stocks["date"] >= lookback_start) &
                                 (self.stocks["date"] <= lookback_end))]
            data_event = DataEvent(next_order_date, batch)
            self.events_queue.put(data_event)

    def find_trade_date(self, current_date, gap):
        '''

        :param current_date: Datetime,
        :param gap: Int, number of days away from current date
                    negative value for future, positive value for past
        :return: Datetime,
        '''

        # Terminate when an invalid date is passed
        if current_date > self.last_date:
            sys.exit("Not a valid date in sample data")
        # While loop to handle scenario that the initial date is not trade date
        while True:
            if self.all_date.isin([current_date]).any():
                break
            else:
                current_date = current_date + pd.Timedelta(days=1)

        # Find index of next date
        current_index = self.all_date[self.all_date == current_date].index[0]
        target_index = current_index + gap

        # Check out of index error
        if target_index < 0:
            return False
        elif target_index > len(self.all_date) - 1:
            return False
        target_date = self.all_date[target_index]
        return target_date

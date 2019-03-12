from abc import ABCMeta, abstractmethod
from sqlalchemy import create_engine
import pandas as pd
from event import DataEvent


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
        df.set_index("ts_code", inplace=True)
        df = df[((df["date"] >= self.start_date) &
                 (df["date"] <= self.end_date))]

        sql_statement = "SELECT * FROM StockBasic"
        result = self.engine.execute(sql_statement)
        keys = result.keys()
        df2 = [data for data in result]
        df2 = pd.DataFrame(df2, columns=keys)
        df2.set_index("ts_code", inplace=True)
        return df, df2

    def stream_next(self, executed_event):
        '''

        :param executed_event:
        :return:
        '''
        last_date = executed_event.datetime
        lookback_end = last_date + pd.Timedelta(days=self.frequency)
        lookback_start = lookback_end - pd.Timedelta(days=self.lookback)
        if lookback_end == self.end_date:
            # handle when backtest end
            pass
        else:
            batch = self.stocks[((self.stocks["date"] >= lookback_start) &
                                 (self.stocks["date"] <= lookback_end))]
            dataevent = DataEvent(lookback_end, batch)
            self.events_queue.put(dataevent)

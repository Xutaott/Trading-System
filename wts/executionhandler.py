from abc import abstractmethod
import pandas as pd
from wts.event import FillEvent, NotFillEvent


class ExecutionHandler(object):
    '''
    ExecutionHandler is a base class providing interface for backtest and
    live execution class that handle order event and generate fill event
    '''

    @abstractmethod
    def execute_order(self, order_event):
        '''

        :param order_event: OrderEvent
        '''
        raise NotImplementedError("Should Implement execute_order method")


class SimExecutionHandler(ExecutionHandler):

    def __init__(self, queue, engine):
        '''
        Handle the order event and generate fill event
        :param queue: Queue
        :param engine: Engine, bind to the database
        '''
        self.events_queue = queue
        self.engine = engine
        self.df = self._load_sql()

    def _load_sql(self):
        '''
        Load data from database
        :return: Dataframe
        '''
        sql_statement = "SELECT * FROM Stocks"
        result = self.engine.execute(sql_statement)
        keys = result.keys()
        df = [data for data in result]
        df = pd.DataFrame(df, columns=keys)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def execute_order(self, order_event):
        '''

        For backtesting, assume the order is executed at close price
        Assume the fee is 0.1%
        TODO: Tailor the executed price and fee
        :param order_event: OrderEvent
        '''
        datetime = order_event.datetime
        symbol = order_event.symbol
        order_type = order_event.order_type
        quantity = order_event.quantity
        side = order_event.side

        current_df = self.df[((self.df["date"] == datetime) &
                              (self.df["ts_code"] == symbol))]

        if len(current_df) == 0:
            notfill_event = NotFillEvent(datetime)
            self.events_queue.put(notfill_event)
        else:
            executed_price = current_df["close_p"].values[0]
            fee = executed_price * quantity * 0.001
            fill_event = FillEvent(datetime, symbol, quantity,
                                   executed_price, side, fee)
            self.events_queue.put(fill_event)


class LiveExecutionHandler(ExecutionHandler):

    # TODO
    def __init__(self, queue):
        self.events_queue = queue

    # TODO
    def execute_order(self, order_event):
        pass

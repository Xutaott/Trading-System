from abc import abstractmethod
import numpy as np
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

    def __init__(self, queue, datahandler):
        '''
        Handle the order event and generate fill/notfill event
        :param queue: Queue
        :param datahandler: DailyDataHandler
        '''
        self.events_queue = queue
        self.datahandler = datahandler
        self.close = self.datahandler.get_data("close")
        self.symbol, self.date, self.valid = self.datahandler.get_available()

    def execute_order(self, order_event):
        '''

        For backtesting, assume the order is executed at close price
        Assume the fee is 0.1%
        TODO: Tailor the executed price and fee
        :param order_event: OrderEvent
        '''
        didx = order_event.didx
        symbol = order_event.symbol
        order_type = order_event.order_type
        quantity = order_event.quantity
        side = order_event.side
        arg = np.where(self.symbol == symbol)
        current_close = self.close[didx, arg[0]][0]

        if np.isnan(current_close):  # The stock is not traded today
            notfill_event = NotFillEvent(didx)
            self.events_queue.put(notfill_event)
        else:
            executed_price = current_close
            fee = executed_price * quantity * 0.001
            fill_event = FillEvent(didx, symbol, quantity,
                                   executed_price, side, fee)
            self.events_queue.put(fill_event)


class LiveExecutionHandler(ExecutionHandler):

    # TODO
    def __init__(self, queue):
        self.events_queue = queue

    # TODO
    def execute_order(self, order_event):
        pass

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

    def __init__(self, queue, datahandler, spread=0.001, fee=0.0005):
        '''
        Handle the order event and generate fill/notfill event
        :param queue: Queue
        :param datahandler: DailyDataHandler
        :param spread: float, percentage of the price
        :param fee: float, percentage of the price
        '''
        self.events_queue = queue
        self.datahandler = datahandler
        self.spread = spread
        self.fee = fee
        self.close = self.datahandler.get_data("close_p")
        self.symbol, self.date, self.valid = self.datahandler.get_available()

    def execute_order(self, order_event):
        '''
        TODO: Tailor the executed price and fee
        TODO: Tailor the oder_type
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
            if side == "Buy":
                executed_price = current_close * (1+self.spread)
            else:
                executed_price = current_close * (1-self.spread)
            fee = executed_price * quantity * self.fee
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

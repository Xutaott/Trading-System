from abc import ABCMeta, abstractmethod
from wts.holding import Holding
from wts.position import Position
from wts.event import OrderEvent, NextLoopEvent
import pandas as pd
import copy


class PortfolioHandler(object):
    '''
    Portfolio is a base class providing interface for backtest and
    live trading class that handle signal event and generate order event
    '''

    _metaclass__ = ABCMeta

    @abstractmethod
    def update_order(self, signal_event):
        raise NotImplementedError("Should Implement update_order method")

    @abstractmethod
    def update_fill(self, fill_event):
        raise NotImplementedError("Should Implement update_fill method")


class SimPortfolioHandler(PortfolioHandler):
    '''
    For backtest purpose
    '''

    def __init__(self, queue, first_date, initial_cap=1000000.0):
        '''

        :param queue: Queue
        :param initial_cap: Initial capital for backtest
        '''
        self.events_queue = queue
        self.position = Position()
        self.holding = Holding(cash=initial_cap)

        self.total_order = 0
        self.process_order = 0

        self.all_position = dict()
        self.all_holding = dict()

    def update_order(self, signal_event):
        '''
        Based on signal event, generate many order events and put it in queue
        :param signal_event: SignalEvent
        '''
        # Get target position
        date_signal = signal_event.datetime
        signal = signal_event.signal
        target_position = self.get_target_position(signal)
        # Generate orders
        orders_dict = self.order_generator(target_position)
        self.total_order = len(orders_dict)
        # If no order, then position and holding do not change
        if self.total_order == 0:
            self.all_holding[date_signal] = copy.deepcopy(self.holding)
            self.all_position[date_signal] = copy.deepcopy(self.position)
            nextloop_event = NextLoopEvent(date_signal)
            self.events_queue.put(nextloop_event)
        else:
            # Convert orders to order event
            for symbol in orders_dict:
                order = orders_dict[symbol]
                order_event = OrderEvent(date_signal, symbol,
                                         order["quantity"],
                                         order["side"],
                                         order["price_range"])
                self.events_queue.put(order_event)

    # Private function
    def get_target_position(self, signal):
        '''
        Based on signal from alpha strategy and current holding, and
        apply risk and asset management, generate target position
        TODO :*VERY IMPORTANT* Could explore more in the future
        # Assume we long 200 shares for every symbol in the signal
        # Also assume enough capital
        :param signal: dict
        :return: dict of dict {String symbol: {Int quantity, String side}}
        '''
        target_position = dict()
        for symbol in signal:
            target_position[symbol] = dict(quantity=200, side="Buy")
        return target_position

    # Private function
    def order_generator(self, target_position):
        '''
        Compare target position and current position, generate orders
        :param target_position: dict,
        :return: dict of dict, {String symbol:{String order_type, Int quantity,
                String side}}
        '''
        order_dict = dict()

        # For symbols in target position,
        # We need to adjust its quantity in current position
        for symbol, target in target_position.items():
            current = self.position.get_position(symbol)
            delta_quantity = target["quantity"] - current["quantity"]
            if delta_quantity > 0:
                order_dict[symbol] = {"order_type": "Market",
                                      "quantity": delta_quantity,
                                      "side": "Buy",
                                      "price_range": None}
            elif delta_quantity < 0:
                order_dict[symbol] = {"order_type": "Market",
                                      "quantity": -delta_quantity,
                                      "side": "Sell",
                                      "price_range": None}

        # target position only include symbols in the signal event
        # So for symbols only in current position, we need to sell them
        current_position = self.position.dictionary
        for symbol in current_position:
            if symbol not in target_position:
                delta_quantity = current_position[symbol]["quantity"]
                if delta_quantity == 0:
                    continue
                else:
                    order_dict[symbol] = {"order_type": "Market",
                                          "quantity": delta_quantity,
                                          "side": "Sell",
                                          "price_range": None}
        return order_dict

    def update_fill(self, event):
        '''
        Based on execution, update position and holding
        :param event: FillEvent or NotFillEvent
        '''
        self.process_order += 1
        # Update position
        date = event.datetime
        delta_amount, fee = self.position.update_position(event)
        self.holding.update_holding(delta_amount, fee)

        # When all orders in current period are filled, record position and
        # holding, and then generate next loop event
        if self.process_order == self.total_order:
            self.all_position[date] = copy.deepcopy(self.position)
            self.all_holding[date] = copy.deepcopy(self.holding)

            nextloop_event = NextLoopEvent(date)
            self.events_queue.put(nextloop_event)

            self.total_order = 0
            self.process_order = 0

    def to_dataframe(self):
        '''

        :return: DataFrame of positions, DataFrame of holdings
        '''
        all_holding = []
        for date, holding in self.all_holding.items():
            df = holding.to_dataframe()
            df["datetime"] = date
            all_holding.append(df)
        all_holding = pd.concat(all_holding)

        all_position = []
        for date, position in self.all_position.items():
            df = position.to_dataframe()
            df["datetime"] = date
            all_position.append(df)
        all_position = pd.concat(all_position)

        return all_holding, all_position


# TODO: Implement live trading handler
class LivePortfolioHandler(PortfolioHandler):
    pass

# coding=utf-8

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

    def __init__(self, queue, freq=20, initial_cap=10000000.0,
                 leverage=0.8, allo='AD'):
        '''

        :param queue: Queue
        :param freq: Int, frequency of rebalancing position
        :param initial_cap: Float, initial capital for backtest
        :param leverage: float, percentage of capital allocated to stocks
        :param allo: string, capital allocation method
        '''
        self.events_queue = queue
        self.freq = freq
        self.position = Position()
        self.holding = Holding(cash=initial_cap)
        self.leverage = leverage
        self.allo = allo

        self.total_order = 0
        self.process_order = 0
        # Record historical holding and postion for performance metrics
        self.all_position = dict()
        self.all_holding = dict()
        # Record historical orders for portfolio construction analysis
        self.all_order = dict()

    # Based on signal event, generate many order events and put it in queue
    def update_order(self, signal_event):
        '''

        :param signal_event: SignalEvent
        '''
        # Get target position, according to specified PM strategy
        didx = signal_event.didx
        signal = signal_event.signal
        next_didx = didx + self.freq

        if self.allo == "AD":  # AD for amount divesified
            target_position = self.get_target_position_ad(signal)
        else:
            target_position = self.get_target_position(signal)

        # Generate orders
        orders_dict = self.order_generator(target_position)
        # Record orders
        self.all_order[didx] = orders_dict
        self.total_order = len(orders_dict)
        # If no order, then position and holding do not change
        if self.total_order == 0:
            self.all_holding[didx] = copy.deepcopy(self.holding)
            self.all_position[didx] = copy.deepcopy(self.position)
            nextloop_event = NextLoopEvent(next_didx)
            self.events_queue.put(nextloop_event)
        else:
            # Convert orders to order event
            for symbol in orders_dict:
                order = orders_dict[symbol]
                order_event = OrderEvent(didx, symbol,
                                         order["quantity"],
                                         order["side"],
                                         order["price_range"])
                self.events_queue.put(order_event)

    # Get target position, Naive version
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

    # Get target position, amount diversified version
    def get_target_position_ad(self, signal):
        '''
        # TODO: divide into several group and generate holdings
        # TODO: and positions for each group
        # Allocate 80% of capital in stocks, amount diversified in each stock
        :param signal: dict
        :return: dict of dict {String symbol: {Int quantity, Float pre_close}}
        '''
        # Calculate capital allocated to each stock
        capital = self.holding.dictionary["total"]
        stock = capital * self.leverage
        each_stock = stock / len(signal)

        target_position = dict()
        for i in signal:
            signal_i = signal[i]
            pre_close = signal_i["pre_close"]
            quantity = int(round(each_stock / pre_close, -2))
            target_position[signal_i["symbol"]] = dict(quantity=quantity,
                                                       pre_close=pre_close)
        '''
        for symbol in signal:
            last_close = signal[symbol]["last_close"]
            quantity = int(round(each_stock / last_close, -2))
            target_position[symbol] = dict(quantity=quantity, side="Buy")
        '''
        return target_position

    # Compare target position and current position, generate orders
    # Update the price of stock whose position does not change
    def order_generator(self, target_position):
        '''
        # TODO: generate price_range based on pre_close to manage spread risk
        :param target_position: dict,
        :return: dict of dict, {String symbol:{String order_type, Int quantity,
                String side}}
        '''
        order_dict = dict()

        # For symbols in target position,
        # We need to adjust quantity and update price in current position
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
            elif delta_quantity == 0:
                initial_amount = current['amount']
                new_price = target['pre_close']
                current['current_price'] = new_price
                current["amount"] = current["quantity"] * current[
                    "current_price"]
                delta_stock = current['amount'] - initial_amount
                self.holding.update_holding(0.0, delta_stock, 0.0)

        # target position only include symbols in the signal event
        # So for symbols only in current position, we need to sell them
        current_position = self.position.dictionary
        for symbol in current_position:
            if symbol not in target_position:
                quantity = current_position[symbol]["quantity"]
                order_dict[symbol] = {"order_type": "Market",
                                      "quantity": quantity,
                                      "side": "Sell",
                                      "price_range": None}
        return order_dict

    # Based on execution, update position and holding
    def update_fill(self, event):
        '''

        :param event: FillEvent or NotFillEvent
        '''
        self.process_order += 1
        # Update position and holding
        delta_cash, delta_stock, fee = self.position.update_position(event)
        self.holding.update_holding(delta_cash, delta_stock, fee)

        # When all orders in current period are filled, record position and
        # holding, and then generate next loop event
        if self.process_order == self.total_order:
            didx = event.didx
            next_didx = didx + self.freq
            self.all_position[didx] = copy.deepcopy(self.position)
            self.all_holding[didx] = copy.deepcopy(self.holding)

            nextloop_event = NextLoopEvent(next_didx)
            self.events_queue.put(nextloop_event)

            self.total_order = 0
            self.process_order = 0

    # Output to dataframe
    def to_dataframe(self):
        '''

        :return: DataFrame of positions, DataFrame of holdings
        '''
        all_holding = []
        for didx, holding in self.all_holding.items():
            df = holding.to_dataframe()
            df["didx"] = didx
            all_holding.append(df)
        all_holding = pd.concat(all_holding)
        all_holding.set_index('didx', inplace=True)

        all_position = []
        for didx, position in self.all_position.items():
            df = position.to_dataframe()
            df["didx"] = didx
            all_position.append(df)
        all_position = pd.concat(all_position)
        all_position.set_index('didx', inplace=True)

        all_order = []
        for didx, orders in self.all_order.items():
            df = pd.DataFrame.from_dict(orders, orient='index')
            df.reset_index(inplace=True)
            df['didx'] = didx
            all_order.append(df)
        all_order = pd.concat(all_order)
        all_order.set_index('didx', inplace=True)

        return all_holding, all_position, all_order


# TODO: Implement live trading handler
class LivePortfolioHandler(PortfolioHandler):
    pass

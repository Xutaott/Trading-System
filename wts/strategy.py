# coding=utf-8

from abc import ABCMeta, abstractmethod
from wts.event import SignalEvent
import numpy as np


class Strategy():
    '''
        Strategy is a base class providing an interface for all inherited
        strategy to generate backtesting trading signals
    '''
    __metaclass__ = ABCMeta

    # Update the signals for next loop
    @abstractmethod
    def update_signal(self, nextloop_event):
        '''
        :param nextloop_event: NextLoopEvent
        '''
        raise NotImplementedError("Should Implement update_signal method")

    # Get a vector of alpha values
    @abstractmethod
    def alpha_generator(self, didx):
        '''
        Implement your alpha strategy
        :param didx: Integer, index of trading date
        :return: np.array, an array of trading signal
        '''
        raise NotImplementedError("Should Implement alpha_generator method")

    # Get trading signal
    @abstractmethod
    def signal_generator(self, alpha_decay, didx):
        '''
        Generate signal based on alpha values
        :param alpha_decay: np.array
        :param didx: Int
        :return Dict of dict, {Int rank: {"symbol":String,"side": String,
                                        "pre_close": float}}
        '''
        raise NotImplementedError("Should Implement signal_generator method")


class AlphaBase(Strategy):
    '''
    Stock Selection
    '''

    def __init__(self, datahandler, events_queue, lookback=10, decay=10, N=50,
                 ema=False):
        '''

        :param datahandler: DataHandler
        :param events_queue: Queue
        :param lookback: int, days of rolling window
        :param decay: int, days of EMA decay to smooth the alpha
        :param N: int, number of stocks to long
        :param ema: boolean, whether to exponential smooth the alpha
        '''
        self.datahandler = datahandler
        self.events_queue = events_queue
        self.lookback = lookback
        self.decay = decay
        self.N = N
        self.symbol, self.date, self.valid = self.datahandler.get_available()
        self.close = self.datahandler.get_data("close_p")
        self.last_didx = self.close.shape[0] - 1
        self.ema = ema
        self.ema_alpha = 0.5

    def update_signal(self, nextloop_event):
        didx = nextloop_event.didx
        # Handle the starting scenario
        if didx < self.lookback + self.decay:
            didx = self.lookback + self.decay
        # Handle the ending scenario
        if didx >= self.last_didx:
            return "Backtest End"

        # Update the smoothed alpha
        alpha_decay = self.alpha_smooth(didx, self.ema)

        # Generate signal from smoothed alpha
        signal = self.signal_generator(alpha_decay, didx)
        signal_event = SignalEvent(didx, signal)
        self.events_queue.put(signal_event)

    # Need to overide to specify the alpha formula
    def alpha_generator(self, didx):
        # To avoid future infor leak,the last available index should be didx-1
        # e.x. self.close[startdidx:didx, v], startdidx = didx - lookback
        # e.x. self.close[didx-1, v]

        # Following are sample format of an alpha strategy
        num_Insts = len(self.symbol)
        alpha = np.array([np.nan] * num_Insts)
        # v = [True] * num_Insts
        # for i in range(1, self.lookback + 1):
        #     v1 = self.valid[didx - i, :]
        #     v = np.logical_and(v, v1)
        # startdidx = didx - self.lookback
        #
        # # Pay attention to "axis" parameter
        # buy_sm_vol = self.buy_sm_vol[startdidx:didx, v]
        # alpha[v] = np.nanmean(buy_sm_vol, axis=0) / np.nanstd(buy_sm_vol,
        #                                                       axis=0)
        return alpha

    # function to smooth the alpha
    def alpha_smooth(self, didx, ema=True):

        alpha_records = []
        for i in range(self.decay):
            alpha = self.alpha_generator(didx - i)
            alpha_records.append(alpha.reshape(1, -1))
        alpha_records = np.concatenate(alpha_records, axis=0)
        if ema:
            ema_weight = self.ema_weight()
            alpha_decay = np.nansum(alpha_records * ema_weight, axis=0)
        else:
            alpha_decay = np.nanmean(alpha_records, axis=0)
        return alpha_decay

    def signal_generator(self, alpha_decay, didx):
        pre_close = self.close[didx - 1]
        # Convert inf to nan, inf is from divide by 0 error
        alpha_decay[alpha_decay == np.inf] = np.nan
        # Return an array of indices that sort the array in descending order
        # np.nan in the last
        argsort = np.argsort(-alpha_decay)
        signal = dict()
        for i in range(self.N):
            symbol = self.symbol[argsort[i]]
            pre_close_i = pre_close[argsort[i]]
            # TODO: FIX BUG of Valid Matrix
            if np.isnan(pre_close_i):
                continue
            signal[i + 1] = {"symbol": symbol, "side": "Buy",
                             "pre_close": pre_close_i}
        return signal

    def get_data(self, keyword):
        '''

        :param keyword: string
        :return: T*M numeric matrix
        '''
        return self.datahandler.get_data(keyword)

    def get_index(self, keyword, ts_code):
        '''

        :param keyword: string
        :param ts_code: string
        :return: 1*M array
        '''
        return self.datahandler.get_index(keyword, ts_code)

    # Function to calculate the ema weight of alpha
    def ema_weight(self):
        weight = [self.ema_alpha]
        for i in range(self.decay - 1):
            weight.append(weight[-1] * (1 - self.ema_alpha))
        weight = np.array(weight).reshape(-1, 1)
        weight_matrix = np.hstack([weight] * len(self.symbol))
        return weight_matrix

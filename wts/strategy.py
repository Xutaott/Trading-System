from abc import ABCMeta, abstractmethod
from wts.event import SignalEvent
import pandas as pd
import numpy as np


class Strategy():
    '''
        Strategy is a base class providing an interface for all inherited
        strategy to generate trading signals
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
    def signal_generator(self, alpha, didx, N=50):
        '''
        Generate signal based on alpha values
        :param alpha: np.array
        :param didx: Int
        :param N: number of stock to long
        :return Dict of dict, {Int rank: {"symbol":String,"side": String,
                                        "pre_close": float}}
        '''
        raise NotImplementedError("Should Implement signal_generator method")

    # @abstractmethod
    # def backtest_sim(self, start_date, end_date, engine):
    #     '''
    #     Initialize the backtest simulation
    #     :param start_date: String
    #     :param end_date: String
    #     :param engine: Engine, bind to the stock database
    #     '''
    #     raise NotImplementedError("Should Implement backtest_sim method")


class AlphaBase(Strategy):
    '''
    Stock Selection
    '''

    def __init__(self, datahandler, events_queue, lookback=1):
        '''

        :param datahandler: DataHandler_matrix
        :param events_queue: Queue
        :param lookback: Int, days of rolling window
        '''
        self.datahandler = datahandler
        self.events_queue = events_queue
        self.lookback = 10
        self.symbol, self.date, self.valid = self.datahandler.get_available()
        self.close = self.datahandler.get_data("close_p")
        self.last_didx = self.close.shape[0] - 1
        print(self.last_didx)

        # For test purpose
        # self.open = self.datahandler.get_data("open_p")
        # self.high = self.datahandler.get_data("high_p")
        # self.low = self.datahandler.get_data("low_p")
        # self.vol = self.datahandler.get_data("volume")
        self.buy_sm_vol = self.datahandler.get_data("buy_sm_vol")

    def update_signal(self, nextloop_event):
        didx = nextloop_event.didx
        # Handle the starting scenario
        if didx < self.lookback:
            didx = self.lookback
        # Handle the ending scenario
        if didx >= self.last_didx:
            return "Backtest End"
        alpha = self.alpha_generator(didx)
        signal = self.signal_generator(alpha, didx)
        signal_event = SignalEvent(didx, signal)
        self.events_queue.put(signal_event)

    def alpha_generator(self, didx):
        # To avoid future infor, the last available index should be didx - 1
        # e.x. self.close[startdidx:didx, v], startdidx = didx - lookback
        # e.x. self.close[didx-1, v]
        num_Insts = len(self.symbol)
        alpha = np.array([np.nan] * num_Insts)
        v = [True] * num_Insts
        for i in range(1, self.lookback + 1):
            v1 = self.valid[didx - i, :]
            v = np.logical_and(v, v1)
        startdidx = didx - self.lookback
        # Pay attention to "axis" parameter
        # alpha[v] = np.max(self.close[startdidx:didx, v] /
        #                  self.open[startdidx:didx, v], axis=0)
        # volume = self.vol[startdidx:didx, v]
        # alpha[v] =  volume[-1] / np.nanmean(volume, axis=0)
        # close = self.close[didx-1, v]
        # high = self.high[didx-1, v]
        # low = self.low[didx-1, v]
        buy_sm_vol = self.buy_sm_vol[startdidx:didx, v]
        alpha[v] = np.nanmean(buy_sm_vol, axis=0) / np.nanstd(buy_sm_vol,
                                                              axis=0)
        return alpha

    def signal_generator(self, alpha, didx, N=50):
        pre_close = self.close[didx - 1]
        # Return an array of indices that sort the array in descending order
        # np.nan in the last
        argsort = np.argsort(-alpha)
        # Assume we long the top 50 stocks
        signal = dict()
        for i in range(N):
            symbol = self.symbol[argsort[i]]
            pre_close_i = pre_close[argsort[i]]
            signal[i + 1] = {"symbol": symbol, "side": "Buy",
                             "pre_close": pre_close_i}
        return signal

# class DailyStrategy(Strategy):
#     '''
#     Strategy that takes OHLCV data as input to generate signals
#     '''
#
#     def __init__(self, events_queue, lookback=1, frequency=1):
#         '''
#
#         :param lookback: Int, days of rolling window
#         :param frequency: Int, frequency (in days) to rebalance
#         :param events_queue: Queue
#         '''
#         self.lookback = lookback
#         self.frequency = frequency
#         self.events_queue = events_queue
#
#     def update_signal(self, data_event):
#         date = data_event.datetime
#         batch = data_event.batch
#
#         signal = self.signals_generator(batch)
#         signal_event = SignalEvent(date, signal)
#         self.events_queue.put(signal_event)
#
#     def signals_generator(self, batch, percent=0.01):
#         '''
#         Key method. Implement the strategy
#         TODO: Generalize a calculation method, it could take a alpha
#         TODO: equation and long the top 5% automatically
#         :param batch: Dataframe
#         :param percent: percentage of number of stocks
#         :return: Dict of dict, {String symbol:
#                                     {"side": String, "last_close": float}}
#         '''
#         '''
#         # For testing, assume long the first 4 stock
#         symbol_list = np.sort(batch["ts_code"].unique())
#         target_symbol = symbol_list[:4]
#         signal = dict()
#         for symbol in target_symbol:
#             batch_symbol = batch[batch["ts_code"] == symbol]
#             batch_symbol = batch_symbol.sort_values(by="date",
#                                                    ascending=False)
#             last_close = batch_symbol.head(1)["close_p"].values[0]
#             signal[symbol] = {"side": "Buy", "last_close": last_close}
#
#         '''
#         symbol_list = batch["ts_code"].unique()
#         num = int(round(len(symbol_list) * percent, 0))
#         df_alpha = self.alpha(batch)
#         # print(df_alpha)
#         target_symbol = df_alpha.index[:num]
#         signal = dict()
#         for symbol in target_symbol:
#             # Get the last close price
#             batch_symbol = batch[batch["ts_code"] == symbol]
#             batch_symbol = batch_symbol.sort_values(by="date",
#                                                     ascending=False)
#             last_close = batch_symbol.head(1)["close_p"].values[0]
#             signal[symbol] = {"side": "Buy", "last_close": last_close}
#         return signal
#
#     # alpha strategy to find target symbol
#     def alpha(self, batch):
#         '''
#
#         :param batch: Dataframe
#         :return: Dataframe
#         '''
#         batch = batch.set_index("ts_code")
#         df = (batch["high_p"] + batch["low_p"]) / 2.0 - batch["close_p"]
#         df = pd.DataFrame(df, columns=["alpha"])
#         df.sort_values(by="alpha", ascending=False, inplace=True)
#         return df

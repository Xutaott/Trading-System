# coding=utf-8


import numpy as np
import time

from wts.run import run, datahandler, events_queue
from wts.strategy import AlphaBase

'''
Guideline for backtesting process
1. In run.py, specify the backtesting time range and other parameter
2. Define a new Alpha class, override init, specify the rolling window,
   smooth decay window, number of long stocks, data you will use;
   N is default to be 20
3. Override alpha_generator, specify the calculation process
4. If performance metrics are good enough, generate a new folder in 'alpha'
   to store metrics and alpha formula
'''


class Alpha1(AlphaBase):
    def __init__(self, datahandler, events_queue):
        super(Alpha1, self).__init__(datahandler, events_queue)

        self.lookback = 5
        self.decay = 30
        # Set up the ema if needed
        # self.ema = True
        # self.ema_alpha = 0.6

        self.pre_close = self.get_data('pre_close')
        self.high_p = self.get_data('high_p')
        self.low_p = self.get_data('low_p')
        self.daily_return = self.close / self.pre_close - 1

    def alpha_generator(self, didx):
        num_Insts = len(self.symbol)
        alpha = np.array([np.nan] * num_Insts)
        v = [True] * num_Insts
        for i in range(1, self.lookback + 1):
            v1 = self.valid[didx - i, :]
            v = np.logical_and(v, v1)
        startdidx = didx - self.lookback

        high_p = self.high_p[startdidx:didx, v]
        low_p = self.low_p[startdidx:didx, v]
        daily_return = self.daily_return[startdidx, v]
        alpha[v] = np.argsort(daily_return * (high_p - low_p), axis=0)[-1, :]

        return alpha


start = time.time()

strategy = Alpha1(datahandler, events_queue)
run(strategy)

end = time.time()
print('%s seconds' % (end - start))

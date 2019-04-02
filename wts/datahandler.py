from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
import time


class DataHandler(object):
    '''
    DataHandler is a base class providing an interface for all inherited
    data handler to stream data to strategy in time sequence
    '''
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_data(self, keyword):
        '''

        :param keyword: String
        :return: np.array, matrix of data
        '''
        raise NotImplementedError("Should Implement stream_next method")


class DailyDataHandler(DataHandler):
    '''
    Specific for stock selection strategy
    '''

    def __init__(self, engine_stock, start_dt, end_dt):
        '''

        :param engine_stock: sqlalchemy.engine
        :param start_dt: String, '20100101'
        :param end_dt: String
        # TODO: engine for index
        '''
        self.engine_stock = engine_stock
        self.start_dt = start_dt
        self.end_dt = end_dt

    # Get array of business date, and matrix of tradable stocks
    def get_available(self):
        '''

        :return: np.array: symbol
                 np.array: business date
                 np.array: matrix of tradable stocks
        '''
        # t1 = time.time()
        sql = "SELECT date, ts_code, close_p FROM Stocks WHERE date " \
              "BETWEEN %s AND %s " \
              "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
        result = self.engine_stock.execute(sql)
        data = result.fetchall()
        keys = result.keys()
        # t2 = time.time()
        # print("FROM SQL: %s seconds"%(t2-t1))

        df = pd.DataFrame(data, columns=keys)
        df.set_index('date', inplace=True)
        # t3 = time.time()
        # print("SQL Result to dataframe: %s seconds"%(t3-t2))

        df_group = df.groupby('ts_code')
        # t4 = time.time()
        # print("Grouping: %s seconds" % (t4 - t3))

        df_all = []
        symbol = []
        for ts_code, df1 in df_group:
            symbol.append(ts_code)
            df_all.append(df1)
        df_all = pd.concat(df_all, join='outer', axis=1)
        # t5 = time.time()
        # print("Grouping result to new dataframe: %s seconds" % (t5 - t4))

        df_all.drop('ts_code', axis=1, inplace=True)
        date = df_all.index
        symbol = np.array(symbol)
        matrix_all = df_all.as_matrix()
        valid = np.invert(np.isnan(matrix_all))
        # t6 = time.time()
        # print("Dataframe to matrix: %s seconds" % (t6 - t5))

        return symbol, date, valid

    # Get data in matrix form
    def get_data(self, keyword):
        # Daily Bar Data
        if keyword == 'close':
            sql = "SELECT date, ts_code, close_p FROM Stocks WHERE date " \
                  "BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'open':
            sql = "SELECT date, ts_code, open_p FROM Stocks WHERE date " \
                  "BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'high':
            sql = "SELECT date, ts_code, high_p FROM Stocks WHERE date " \
                  "BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'low':
            sql = "SELECT date, ts_code, low_p FROM Stocks WHERE date " \
                  "BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'volume':
            sql = "SELECT date, ts_code, volume FROM Stocks WHERE date " \
                  "BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'amount':
            sql = "SELECT date, ts_code, amount FROM Stocks WHERE date " \
                  "BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'pre_close':
            sql = "SELECT date, ts_code, pre_close FROM Stocks WHERE date " \
                  "BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)

        # Daily Moneyflow data
        if keyword == 'buy_sm_vol':
            sql = "SELECT date, ts_code, buy_sm_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'buy_sm_amount':
            sql = "SELECT date, ts_code, buy_sm_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'sell_sm_vol':
            sql = "SELECT date, ts_code, sell_sm_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'sell_sm_amount':
            sql = "SELECT date, ts_code, sell_sm_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'buy_md_vol':
            sql = "SELECT date, ts_code, buy_md_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'buy_md_amount':
            sql = "SELECT date, ts_code, buy_md_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'sell_md_vol':
            sql = "SELECT date, ts_code, sell_md_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'sell_md_amount':
            sql = "SELECT date, ts_code, sell_md_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'buy_lg_vol':
            sql = "SELECT date, ts_code, buy_lg_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'buy_lg_amount':
            sql = "SELECT date, ts_code, buy_lg_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'sell_lg_vol':
            sql = "SELECT date, ts_code, sell_lg_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'sell_lg_amount':
            sql = "SELECT date, ts_code, sell_lg_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'buy_elg_vol':
            sql = "SELECT date, ts_code, buy_elg_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'buy_elg_amount':
            sql = "SELECT date, ts_code, buy_elg_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'sell_elg_vol':
            sql = "SELECT date, ts_code, sell_elg_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'sell_elg_amount':
            sql = "SELECT date, ts_code, sell_elg_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'net_mf_vol':
            sql = "SELECT date, ts_code, net_mf_vol FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'net_mf_amount':
            sql = "SELECT date, ts_code, net_mf_amount FROM Moneyflow " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)

        # Daily Technical data
        if keyword == 'turnover_rate':
            sql = "SELECT date, ts_code, turnover_rate FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'turnover_rate_f':
            sql = "SELECT date, ts_code, turnover_rate_f FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'volume_ratio':
            sql = "SELECT date, ts_code, volume_ratio FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'pe_lyr':
            sql = "SELECT date, ts_code, pe_lyr FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'pe_ttm':
            sql = "SELECT date, ts_code, pe_ttm FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'pb':
            sql = "SELECT date, ts_code, pb FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'ps_lyr':
            sql = "SELECT date, ts_code, ps_lyr FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'ps_ttm':
            sql = "SELECT date, ts_code, ps_ttm FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'total_share':
            sql = "SELECT date, ts_code, total_share FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'float_share':
            sql = "SELECT date, ts_code, float_share FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'free_share':
            sql = "SELECT date, ts_code, free_share FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'total_mv':
            sql = "SELECT date, ts_code, total_mv FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)
        if keyword == 'float_mv':
            sql = "SELECT date, ts_code, float_mv FROM Technical " \
                  "WHERE date BETWEEN %s AND %s " \
                  "ORDER BY ts_code, date" % (self.start_dt, self.end_dt)
            result = self.engine_stock.execute(sql)
            return self._to_np(result)

    def _to_np(self, sql_result):
        data = sql_result.fetchall()
        keys = sql_result.keys()
        df = pd.DataFrame(data, columns=keys)
        df.set_index('date', inplace=True)
        df_group = df.groupby('ts_code')
        output = []
        for ts_code, df1 in df_group:
            output.append(df1)
        output = pd.concat(output, join='outer', axis=1)
        output.drop('ts_code', axis=1, inplace=True)
        output = output.as_matrix()
        return output

#
# class DailyDataHandler(DataHandler):
#     '''
#     Stream daily OHLCV data (open,high,low,close,volume) for backtest
#     It's specific for each strategy
#     '''
#
#     def __init__(self, start_date, end_date, events_queue, engine,
#                  lookback=1, frequency=1):
#         '''
#
#         :param start_date: String, start date of backtest period, "20180101"
#         :param end_date: String, end date of backtest period
#         :param events_queue: Queue
#         :param engine: EngineConnect, engine bind to the database
#         :param lookback: Int, lookback period for this strategy
#         :param frequency: Int, frequency of data stream
#         '''
#         self.start_date = pd.to_datetime(start_date, yearfirst=True)
#         self.end_date = pd.to_datetime(end_date, yearfirst=True)
#         self.events_queue = events_queue
#         self.engine = engine
#         self.lookback = lookback
#         self.frequency = frequency
#         # t1 = time.time()
#         self.stocks, self.stock_basic = self._load_sql(start_date, end_date)
#         # t2 = time.time()
#         # print("%s seconds" % (t2 - t1))
#         self.all_date = pd.Series(
#             np.flip(np.sort(self.stocks["date"].unique()), axis=0))
#         self.all_date.sort_values()
#         index = self.all_date.index
#         self.first_date = self.all_date[index[-1]]
#         self.last_date = self.all_date[index[0]]
#
#     def _load_sql(self, start_date, end_date):
#         '''
#         Load data from stock database
#         :return: dataframe
#         '''
#         # engine = create_engine('sqlite:///sample.db')
#         sql_statement = "SELECT * FROM Stocks WHERE date " \
#                         "BETWEEN %s AND %s" % (start_date, end_date)
#         result = self.engine.execute(sql_statement)
#         keys = result.keys()
#         df = [data for data in result]
#         df = pd.DataFrame(df, columns=keys)
#         df["date"] = pd.to_datetime(df["date"], yearfirst=True)
#         # df = df[((df["date"] >= self.start_date) &
#         #         (df["date"] <= self.end_date))]
#
#         sql_statement = "SELECT * FROM StockBasic"
#         result = self.engine.execute(sql_statement)
#         keys = result.keys()
#         df2 = [data for data in result]
#         df2 = pd.DataFrame(df2, columns=keys)
#         return df, df2
#
#     def stream_next(self, nextloop_event):
#         '''
#         :param nextloop_event: NextLoopEvent
#         '''
#         last_order_date = nextloop_event.datetime
#         next_order_date = self.find_trade_date(last_order_date,
#                                                -self.frequency)
#         if not next_order_date:
#             # No new event in the queue, backtest terminate
#             pass
#
#         else:
#             lookback_end = self.find_trade_date(next_order_date, 1)
#             lookback_start = self.find_trade_date(lookback_end,
#                                                   self.lookback - 1)
#             batch = self.stocks[((self.stocks["date"] >= lookback_start) &
#                                  (self.stocks["date"] <= lookback_end))]
#             data_event = DataEvent(next_order_date, batch)
#             self.events_queue.put(data_event)
#
#     def find_trade_date(self, current_date, gap):
#         '''
#
#         :param current_date: Datetime,
#         :param gap: Int, number of days away from current date
#                     negative value for future, positive value for past
#         :return: Datetime,
#         '''
#
#         # Terminate when an invalid date is passed
#         if current_date > self.last_date:
#             sys.exit("Not a valid date in sample data")
#         # While loop to handle scenario that initial date is not trade date
#         while True:
#             if self.all_date.isin([current_date]).any():
#                 break
#             else:
#                 current_date = current_date + pd.Timedelta(days=1)
#
#         # Find index of next date
#         current_index = self.all_date[self.all_date == current_date].index[0]
#         target_index = current_index + gap
#
#         # Check out of index error
#         if target_index < 0:
#             return False
#         elif target_index > len(self.all_date) - 1:
#             return False
#         target_date = self.all_date[target_index]
#         return target_date

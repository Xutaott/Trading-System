# coding=utf-8

from abc import ABCMeta, abstractmethod
import numpy as np


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
    Specific for systematic stock selection strategy backtesting
    '''

    def __init__(self, engine_inter, start_dt, end_dt):
        '''

        :param engine_inter: sqlalchemy.engine
        :param start_dt: String, '20100101'
        :param end_dt: String
        '''
        self.engine_inter = engine_inter
        self.start_dt = start_dt
        self.end_dt = end_dt

    # Get array of symbol and trading date,
    #  whose order is align with corresponding valid (tradable) matrix
    def get_available(self):
        '''

        :return: 1*M array, 1*T array, T*M matrix
        '''
        # Get list of symbol
        sql_statement = "SELECT symbol FROM \"ts_code\""
        result = self.engine_inter.execute(sql_statement)
        data = result.fetchall()
        symbol = np.array(data).reshape(-1)

        # Get list of trade date, corresponding to order of matrix
        sql_statement = "SELECT date FROM \"valid\" WHERE date " \
                        "BETWEEN \'%s\' AND \'%s\'" % (
                            self.start_dt, self.end_dt)
        result = self.engine_inter.execute(sql_statement)
        data = result.fetchall()
        date = np.array(data).reshape(-1)

        sql_statement = "SELECT data FROM \"valid\" WHERE date " \
                        "BETWEEN \'%s\' AND \'%s\'" % (
                            self.start_dt, self.end_dt)
        result = self.engine_inter.execute(sql_statement)
        data = result.fetchall()
        valid = np.array(data)
        rows = valid.shape[0]
        valid = valid.reshape(rows, -1)

        return symbol, date, valid

    # Get data in matrix form
    def get_data(self, keyword):
        '''

        :param keyword: e.x. 'close_p'
        :return: T*M matrix
        '''
        sql_statement = "SELECT data FROM \"%s\" WHERE date BETWEEN \'%s\' " \
                        "AND \'%s\'" % (keyword, self.start_dt, self.end_dt)
        result = self.engine_inter.execute(sql_statement)
        data = result.fetchall()
        matrix = np.array(data)
        rows = matrix.shape[0]
        matrix = matrix.reshape(rows, -1)

        return matrix

    # Get index data
    def get_index(self, keyword, ts_code):
        '''

        :param keyword: e.x. 'close_p'
        :param ts_code: symbol of index, e.x. '000001.SH'
        :return: 1*T array
        '''
        sql_statement = "SELECT %s FROM \"Index\" WHERE ts_code=\'%s\' " \
                        "AND date BETWEEN \'%s\' AND \'%s\'" % (
                            keyword, ts_code, self.start_dt, self.end_dt)
        result = self.engine_inter.execute(sql_statement)
        data = result.fetchall()
        data = np.array(data).reshape(-1)

        return data

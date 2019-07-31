# coding=utf-8

'''
Build an inter database to accelerate backtest efficiency

Four kinds of table:
    ts_code: M * 1, M is # live stocks,
            its order is aligned with column order in data in following table
    valid: T * 2 (date+data)
           T is #trading date in ascending order: early -> current
           for each row, data is a 1 * M Boolean array where M is #live stocks
    normal daily data: close/open/buy_sm_vol/float_mv/....
           same structure as valid matrix, except that data is 1*M float array
    index: (7*T) * 20, 7 is #main index, T is #trading date, 18 is #data

It takes about 1 hour to update each time
'''

from sqlalchemy import Table, Column, ARRAY, Float, String, Boolean
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
import time

# Declare constant variable
TABLE = ['Stocks', 'Moneyflow', 'Technical']
COLUMN_BAR = ['open_p', 'close_p', 'high_p', 'low_p', 'volume', 'amount',
              'pre_close']
COLUMN_MONEYFLOW = ['buy_sm_vol', 'buy_sm_amount', 'sell_sm_vol',
                    'sell_sm_amount', 'buy_md_vol', 'buy_md_amount',
                    'sell_md_vol', 'sell_md_amount', 'buy_lg_vol',
                    'buy_lg_amount', 'sell_lg_vol', 'sell_lg_amount',
                    'buy_elg_vol', 'buy_elg_amount', 'sell_elg_vol',
                    'sell_elg_amount', 'net_mf_vol', 'net_mf_amount']
COLUMN_TECHNICAL = ['turnover_rate', 'turnover_rate_f', 'volume_ratio',
                    'pe_lyr', 'pe_ttm', 'pb', 'ps_lyr', 'ps_ttm',
                    'total_share', 'float_share', 'free_share',
                    'total_mv', 'float_mv']
START_DATE = '20100101'
# Leave 2 years for out sample test
END_DATE = '20170101'

DatabaseTable = declarative_base()


# DROP all tables in the inter database
def drop_all(engine_inter):
    engine_inter.execute('DROP SCHEMA public CASCADE')
    engine_inter.execute('CREATE SCHEMA public')


# Load data for tscode table and valid table
def load_stock_valid_inter(engine_original, engine_inter, start_date, end_date,
                           co_list):
    # Create table for ts_code and valid
    tscode_table = Table("ts_code", DatabaseTable.metadata,
                         Column("symbol", String))
    tscode_table.create(engine_inter)
    valid_table = Table("valid", DatabaseTable.metadata,
                        Column("date", String), Column("data", ARRAY(Boolean)))
    valid_table.create(engine_inter)

    # Sort by ts_code and date to ensure order in rows and columns
    sql_statement = "SELECT date,ts_code,close_p FROM \"Stocks\" WHERE date " \
                    "BETWEEN \'%s\' AND \'%s\' ORDER BY " \
                    "ts_code, date" % (start_date, end_date)
    result = engine_original.execute(sql_statement)
    data = result.fetchall()
    keys = result.keys()
    df = pd.DataFrame(data, columns=keys)
    df.set_index("date", inplace=True)

    df_group = df.groupby("ts_code")
    group = []
    columns = []
    for ts_code, df1 in df_group:
        if ts_code in co_list:
            columns.append(ts_code)
            group.append(df1)
    # Convert to T*M dataframe, use outer join to ensure aligned dimension
    # Mission value (not listed or no trade) is filled by np.nan
    df_all = pd.concat(group, join="outer", axis=1)
    df_all.drop("ts_code", axis=1, inplace=True)
    df_all.columns = columns
    date = df_all.index
    # Convert to a boolean matrix
    matrix_all = df_all.as_matrix()
    valid_matrix = np.invert(np.isnan(matrix_all))

    for i in np.arange(len(columns)):
        engine_inter.execute(tscode_table.insert(), symbol=columns[i])
    # Each row of data is an array, to avoid columns limit in psql
    for i in np.arange(len(date)):
        engine_inter.execute(valid_table.insert(), date=date[i],
                             data=valid_matrix[i, :])


# Load inter bar data
def load_inter_bar_inter(engine_original, engine_inter, start_date, end_date,
                         co_list):
    for column in COLUMN_BAR:
        t1 = time.time()
        print("Load inter %s data" % column)
        mytable = Table(column, DatabaseTable.metadata,
                        Column("date", String),
                        Column("data", ARRAY(Float)))
        mytable.create(engine_inter)

        # Get the matrix from original db
        sql_statement = "SELECT date,ts_code,%s FROM \"Stocks\" WHERE date " \
                        "BETWEEN \'%s\' AND \'%s\' ORDER BY " \
                        "ts_code, date" % (column, start_date, end_date)
        result = engine_original.execute(sql_statement)
        data = result.fetchall()
        keys = result.keys()
        df = pd.DataFrame(data, columns=keys)
        df.set_index("date", inplace=True)

        df_group = df.groupby("ts_code")
        group = []
        columns = []
        for ts_code, df1 in df_group:
            if ts_code in co_list:
                columns.append(ts_code)
                group.append(df1)
        df_all = pd.concat(group, join="outer", axis=1)
        df_all.drop("ts_code", axis=1, inplace=True)
        df_all.columns = columns

        date = df_all.index
        matrix_all = df_all.as_matrix()
        # Insert into inter db
        for i in np.arange(len(date)):
            engine_inter.execute(mytable.insert(), date=date[i],
                                 data=matrix_all[i, :])
        t2 = time.time()
        print(t2 - t1)


# Load inter money flow data
def load_inter_moneyflow_inter(engine_original, engine_inter, start_date,
                               end_date, co_list):
    for column in COLUMN_MONEYFLOW:
        t1 = time.time()
        print("Load inter %s data" % column)
        mytable = Table(column, DatabaseTable.metadata,
                        Column("date", String),
                        Column("data", ARRAY(Float)))
        mytable.create(engine_inter)

        # Get the matrix from original db
        sql_statement = "SELECT date,ts_code,%s FROM \"Moneyflow\" " \
                        "WHERE date BETWEEN \'%s\' AND \'%s\' ORDER BY " \
                        "ts_code, date" % (column, start_date, end_date)
        result = engine_original.execute(sql_statement)
        data = result.fetchall()
        keys = result.keys()
        df = pd.DataFrame(data, columns=keys)
        df.set_index("date", inplace=True)

        df_group = df.groupby("ts_code")
        group = []
        columns = []
        for ts_code, df1 in df_group:
            if ts_code in co_list:
                columns.append(ts_code)
                group.append(df1)
        df_all = pd.concat(group, join="outer", axis=1)
        df_all.drop("ts_code", axis=1, inplace=True)
        df_all.columns = columns

        date = df_all.index
        matrix_all = df_all.as_matrix()

        # Insert into inter db
        for i in np.arange(len(date)):
            engine_inter.execute(mytable.insert(), date=date[i],
                                 data=matrix_all[i, :])
        t2 = time.time()
        print(t2 - t1)


# Load inter technical date
def load_inter_technical_inter(engine_original, engine_inter, start_date,
                               end_date, co_list):
    for column in COLUMN_TECHNICAL:
        t1 = time.time()
        print("Load inter %s data" % column)
        mytable = Table(column, DatabaseTable.metadata,
                        Column("date", String),
                        Column("data", ARRAY(Float)))
        mytable.create(engine_inter)

        # Get the matrix from original db
        sql_statement = "SELECT date,ts_code,%s FROM \"Technical\" " \
                        "WHERE date BETWEEN \'%s\' AND \'%s\' ORDER BY " \
                        "ts_code, date" % (column, start_date, end_date)
        result = engine_original.execute(sql_statement)
        data = result.fetchall()
        keys = result.keys()
        df = pd.DataFrame(data, columns=keys)
        df.set_index("date", inplace=True)

        df_group = df.groupby("ts_code")
        group = []
        columns = []
        for ts_code, df1 in df_group:
            if ts_code in co_list:
                columns.append(ts_code)
                group.append(df1)
        df_all = pd.concat(group, join="outer", axis=1)
        df_all.drop("ts_code", axis=1, inplace=True)
        df_all.columns = columns

        date = df_all.index
        matrix_all = df_all.as_matrix()

        # Insert into inter db
        for i in np.arange(len(date)):
            engine_inter.execute(mytable.insert(), date=date[i],
                                 data=matrix_all[i, :])
        t2 = time.time()
        print(t2 - t1)


# Load inter index data
def load_inter_index_inter(engine_original, engine_inter, start_date,
                           end_date):
    sql_statement = "SELECT * FROM \"Indexs\" WHERE date " \
                    "BETWEEN \'%s\' AND \'%s\' " % (start_date, end_date)
    result = engine_original.execute(sql_statement)
    data = result.fetchall()
    keys = result.keys()
    df1 = pd.DataFrame(data, columns=keys)

    sql_statement = "SELECT * FROM \"TechnicalIndex\" WHERE date " \
                    "BETWEEN \'%s\' AND \'%s\' " % (start_date, end_date)
    result = engine_original.execute(sql_statement)
    data = result.fetchall()
    keys = result.keys()
    df2 = pd.DataFrame(data, columns=keys)

    df = df1.merge(df2, how='outer', on=['date', 'ts_code'])

    df.to_sql(name="Index", con=engine_inter, if_exists="append", index=False)


def find_common(engine_original, start_date, end_date):
    sql_statement = "SELECT DISTINCT ts_code FROM \"Stocks\" " \
                    "WHERE date BETWEEN \'%s\' AND \'%s\'" % (
                        start_date, end_date)
    result = engine_original.execute(sql_statement)
    data = result.fetchall()
    keys = result.keys()
    df = pd.DataFrame(data, columns=keys)
    set_bar = set(df.values.flat)

    sql_statement = "SELECT DISTINCT ts_code FROM \"Moneyflow\" " \
                    "WHERE date BETWEEN \'%s\' AND \'%s\'" % (
                        start_date, end_date)
    result = engine_original.execute(sql_statement)
    data = result.fetchall()
    keys = result.keys()
    df = pd.DataFrame(data, columns=keys)
    set_ml = set(df.values.flat)

    sql_statement = "SELECT DISTINCT ts_code FROM \"Technical\" " \
                    "WHERE date BETWEEN \'%s\' AND \'%s\'" % (
                        start_date, end_date)
    result = engine_original.execute(sql_statement)
    data = result.fetchall()
    keys = result.keys()
    df = pd.DataFrame(data, columns=keys)
    set_tech = set(df.values.flat)

    co_list = list(set_bar.intersection(set_ml).intersection(set_tech))

    return co_list


if __name__ == '__main__':
    # Need to create database first if not exist
    engine_original = create_engine(
        'postgresql://chenxutao:@localhost/chinesestock_pg')
    engine_inter = create_engine(
        'postgresql://chenxutao:@localhost/chinesestock_pg_inter')

    co_list = find_common(engine_original, START_DATE, END_DATE)

    # Drop all table first to avoid duplicate
    drop_all(engine_inter)
    load_stock_valid_inter(engine_original, engine_inter, START_DATE, END_DATE,
                           co_list)
    load_inter_bar_inter(engine_original, engine_inter, START_DATE, END_DATE,
                         co_list)
    load_inter_moneyflow_inter(engine_original, engine_inter, START_DATE,
                               END_DATE, co_list)
    load_inter_technical_inter(engine_original, engine_inter, START_DATE,
                               END_DATE, co_list)
    load_inter_index_inter(engine_original, engine_inter, START_DATE, END_DATE)

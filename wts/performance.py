# coding=utf-8


# TODO: Backtest to build your own alpha pool
# TODO: Construct a portfolio from alphas
# TODO: Put it online

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def find_maxdd(series):
    start = 0
    end = 0
    s = 0
    maxdd_current = 0
    maxdd_all = 0

    for i in np.arange(len(series)):
        maxdd_current += series[i]
        if maxdd_current < maxdd_all:
            maxdd_all = maxdd_current
            start = s
            end = i
        elif maxdd_current > 0:
            maxdd_current = 0
            s = i + 1
    maxdd_all = np.exp(maxdd_all) - 1
    return start, end, maxdd_all


def returns(series):
    return np.exp(np.nansum(series)) - 1


def sharpe(series):
    series = np.exp(series) - 1
    return np.nanmean(series) / np.nanstd(series) * np.sqrt(12)


def metrics_basic(series):
    first_year = series.index[0].year
    last_year = series.index[-1].year
    series.columns = ["return", "riskfree"]

    year_range = np.arange(first_year, last_year + 1)

    return_year = []
    sharpe_year = []
    maxdd_year = []
    date = []
    for year in year_range:
        first_date = pd.to_datetime('%s-01-01' % year)
        last_date = pd.to_datetime('%s-12-31' % year)
        sub_series = series[
            (series.index >= first_date) & (series.index <= last_date)]
        date.append(year)
        return_year.append(returns(sub_series["return"]))
        sharpe_year.append(
            sharpe(sub_series["return"] - sub_series["riskfree"]))
        maxdd_year.append(find_maxdd(sub_series["return"])[2])

    total_return = returns(series["return"])
    return_year.append(total_return)
    sharpe_year.append(sharpe(series["return"] - series["riskfree"]))
    maxdd_year.append(find_maxdd(series["return"])[2])
    date.append("Total")
    df_metrics = pd.DataFrame(list(zip(return_year, sharpe_year, maxdd_year)),
                              index=date,
                              columns=["Returns", "Sharpe", "Max DD"])
    annualized_return = np.power(1 + total_return, 1 / len(year_range)) - 1

    return df_metrics, annualized_return


def backtest_metric():
    holding = pd.read_csv("../backtest_output/all_holding.csv",
                          index_col='date')
    holding.index = pd.to_datetime(holding.index)

    index = pd.read_csv("../database/index.csv", index_col="date")
    index = index[index['ts_code'] == "000001.SH"]

    # TODO: find the appropriate risk free rate to calculate Sharpe
    riskfree = pd.read_csv("../database/riskfree.csv", sep='\t',
                           index_col="Date",
                           usecols=["Date", "Price"])
    riskfree.index = pd.to_datetime(riskfree.index, yearfirst=False)
    riskfree.columns = ["riskfree"]
    riskfree = riskfree / 1200.0

    hedged = holding.merge(index, how="left", left_index=True,
                           right_index=True)

    hedged["log_port"] = np.log(hedged["total"])
    hedged["log_index"] = np.log(hedged["close_p"])
    hedged["return_port"] = hedged["log_port"] - hedged["log_port"].shift(1)
    hedged["return_index"] = hedged["log_index"] - hedged["log_index"].shift(1)
    hedged.dropna(inplace=True)
    hedged = hedged[["return_port", "return_index"]]
    hedged = hedged.merge(riskfree, how="left", left_index=True,
                          right_index=True)

    hedged["return_hedged"] = hedged["return_port"] - hedged["return_index"]

    hedged["culreturn_port"] = np.exp(np.cumsum(hedged["return_port"]))
    hedged["culreturn_index"] = np.exp(np.cumsum(hedged["return_index"]))
    hedged["culreturn_hedged"] = np.exp(np.cumsum(hedged["return_hedged"]))

    df_metrics, annualized_return = metrics_basic(
        hedged[["return_port", "riskfree"]])
    df_metrics = df_metrics.round(3)
    df_metrics.to_csv("../backtest_output/output.csv", index_label="Date")
    print(df_metrics)
    print("%.3f" % (annualized_return))

    df_metrics, annualized_return = metrics_basic(
        hedged[["return_index", "riskfree"]])
    df_metrics = df_metrics.round(3)
    print(df_metrics)
    print("%.3f" % (annualized_return))

    df_metrics, annualized_return = metrics_basic(
        hedged[["return_hedged", "riskfree"]])
    df_metrics = df_metrics.round(3)
    print(df_metrics)
    print("%.3f" % (annualized_return))

    plt.figure(figsize=(10, 5))
    plt.plot(hedged[["culreturn_port", "culreturn_index", "culreturn_hedged"]])
    plt.legend(["Portfolio", "Index", "Hedged"])
    plt.title("Cumulative Return")
    plt.savefig("../backtest_output/CumulativeReturn")

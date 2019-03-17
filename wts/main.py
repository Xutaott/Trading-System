from wts.datahandler import DailyDataHandler
from wts.event import NextLoopEvent
from wts.strategy import DailyStrategy
from wts.portfoliohandler import SimPortfolioHandler
from wts.executionhandler import SimExecutionHandler
from sqlalchemy import create_engine
import queue
import pandas as pd

start_date = "2010-01-01"
end_date = "2011-01-01"
engine = create_engine("sqlite:///../database/sample.db")


def backtest_sim(start_date, end_date, engine):
    events_queue = queue.Queue()
    datahandler = DailyDataHandler(start_date, end_date, events_queue, engine)
    strategy = DailyStrategy(events_queue, lookback=1, frequency=1)
    start_date = pd.Timestamp(start_date)
    first_position_date = datahandler.find_trade_date(start_date,
                                                      -strategy.lookback)

    portfoliohandler = SimPortfolioHandler(events_queue, first_position_date,
                                           initial_cap=1000000.0)
    executionHandler = SimExecutionHandler(events_queue, engine)

    nextloop_event = NextLoopEvent(first_position_date)
    events_queue.put(nextloop_event)

    while not events_queue.empty():
        event = events_queue.get()
        if event.type == "NextLoopEvent":
            datahandler.stream_next(event)

        elif event.type == "DataEvent":
            strategy.update_signal(event)

        elif event.type == "SignalEvent":
            portfoliohandler.update_order(event)

        elif event.type == "OrderEvent":
            executionHandler.execute_order(event)

        elif event.type == "FillEvent" or event.type == "NotFillEvent":
            portfoliohandler.update_fill(event)

    all_holding, all_position = portfoliohandler.to_dataframe()

    all_holding.to_csv("../backtest_output/all_holding.csv")
    all_position.to_csv("../backtest_output/all_position.csv")

    # TODO: do performance analysis
    print("Backtest Finish!")


backtest_sim(start_date, end_date, engine)

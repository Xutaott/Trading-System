import pandas as pd


class Position():

    def __init__(self, position=None):
        '''
        dictionary property is a dict of dict
        {"symbol": {"datetime":Datetime, "quantity":int, "holding_price":float,
        "current_price":float, "cost": float, "amount":float}}
        :param position: instance of last position
        '''
        if position is None:
            self.dictionary = dict()
        else:
            self.dictionary = position.dictionary

    def get_position(self, symbol):
        '''
        :param symbol: String
        :return: a dict that stores the info of current position
        '''
        if symbol not in self.dictionary:
            # Initialize a record if not in current position
            self.dictionary[symbol] = {"datetime": None, "quantity": 0,
                                       "holding_price": 0.0,
                                       "current_price": 0.0,
                                       "cost": 0.0, "amount": 0.0}
        return self.dictionary[symbol]

    def update_position(self, fill_event):
        '''

        :param fill_event: FillEvent
        :return: Float delta_amount, Float fee, for update of holdings
        '''
        symbol = fill_event.symbol
        current = self.get_position(symbol)
        initial_amount = current["amount"]

        if fill_event.side == "Buy":
            current["datetime"] = fill_event.datetime
            current["quantity"] += fill_event.quantity
            current["current_price"] = fill_event.price
            current["amount"] = current["quantity"] * current["current_price"]
            current["cost"] += (fill_event.fee +
                                fill_event.price * fill_event.quantity)
            current["holding_price"] = current["cost"] / float(
                current["quantity"])

        else:
            current["datetime"] = fill_event.datetime
            current["quantity"] -= fill_event.quantity
            current["current_price"] = fill_event.price
            current["amount"] = current["quantity"] * current["current_price"]
            if current["quantity"] != 0:
                current["cost"] += (fill_event.fee -
                                    fill_event.price * fill_event.quantity)
                current["holding_price"] = current["cost"] / float(
                    current["quantity"])
            else:
                current["cost"] = 0
                current["holding_price"] = 0
        delta_amount = current["amount"] - initial_amount
        return delta_amount, fill_event.fee

    def to_dataframe(self):
        '''

        :return: Dataframe, that stores all symbol info of current position
        '''

        df = pd.DataFrame.from_dict(self.dictionary, orient="index")
        df.reset_index(inplace=True)
        df.columns = ["symbol", "datetime", "quantity", "holding_price",
                      "current_price", "cost", "amount"]
        return df

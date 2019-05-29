import pandas as pd


class Position():

    def __init__(self):
        '''
        dictionary property is a dict of dict
        {"symbol": {"quantity":int, "holding_price":float,
        "current_price":float, "cost": float, "amount":float}}
        :param position: instance of last position
        '''
        self.dictionary = dict()

    def get_position(self, symbol):
        '''
        :param symbol: String
        :return: a dict that stores the info of current position
        '''
        if symbol not in self.dictionary:
            # Initialize a record if not in current position
            self.dictionary[symbol] = {"cost": 0.0, "quantity": 0,
                                       "holding_price": 0.0, "amount": 0.0,
                                       "current_price": 0.0}

        return self.dictionary[symbol]

    def update_position(self, event):
        '''

        :param event: FillEvent or NotFillEvent
        :return: Float delta_stock, Float delta_cash, Float fee,
        '''
        delta_stock = 0.0
        delta_cash = 0.0
        # delta_amount = 0.0
        fee = 0.0
        if event.type == "FillEvent":
            fill_event = event
            symbol = fill_event.symbol

            current = self.get_position(symbol)
            initial_amount = current["amount"]

            if fill_event.side == "Buy":
                current["quantity"] += fill_event.quantity
                current["current_price"] = fill_event.price
                current["amount"] = current["quantity"] * current[
                    "current_price"]
                current["cost"] += (fill_event.fee +
                                    fill_event.price * fill_event.quantity)
                current["holding_price"] = current["cost"] / float(
                    current["quantity"])
                delta_cash = - fill_event.quantity * fill_event.price

            else:
                current["quantity"] -= fill_event.quantity
                current["current_price"] = fill_event.price
                current["amount"] = current["quantity"] * current[
                    "current_price"]
                if current["quantity"] != 0:
                    current["cost"] += (fill_event.fee -
                                        fill_event.price * fill_event.quantity)
                    current["holding_price"] = current["cost"] / float(
                        current["quantity"])
                else:
                    current["cost"] = 0
                    current["holding_price"] = 0
                delta_cash = fill_event.quantity * fill_event.price

                if current["quantity"] == 0:
                    self.dictionary.pop(symbol)
            delta_stock = current["amount"] - initial_amount
            fee = fill_event.fee

        return delta_cash, delta_stock, fee

    def to_dataframe(self):
        '''

        :return: Dataframe, that stores all symbol info of current position
        '''

        df = pd.DataFrame.from_dict(self.dictionary, orient="index")
        df.reset_index(inplace=True)

        return df

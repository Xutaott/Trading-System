import pandas as pd


class Position():

    def __init__(self):
        '''
        dictionary property is a dict of dict
        {"symbol": {"quantity":int, "current_price":float, "amount":float}}
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
            self.dictionary[symbol] = {"quantity": 0, "amount": 0.0,
                                       "current_price": 0.0}

        return self.dictionary[symbol]

    def update_position(self, event):
        '''

        :param event: FillEvent or NotFillEvent
        :return: Float delta_stock, Float delta_cash, Float fee,
        '''
        # default scenario is NotFillEvent, no change to position
        delta_stock = 0.0
        delta_cash = 0.0
        fee = 0.0
        if event.type == "FillEvent":
            fill_event = event
            symbol = fill_event.symbol
            fee = fill_event.fee

            current = self.get_position(symbol)
            initial_amount = current["amount"]

            if fill_event.side == "Buy":
                current["quantity"] += fill_event.quantity
                current["current_price"] = fill_event.price
                current["amount"] = current["quantity"] * current[
                    "current_price"]
                delta_cash = - fill_event.quantity * fill_event.price

            else:
                current["quantity"] -= fill_event.quantity
                current["current_price"] = fill_event.price
                current["amount"] = current["quantity"] * current[
                    "current_price"]
                delta_cash = fill_event.quantity * fill_event.price

            # drop the stock position if its quantity == 0
            if current["quantity"] == 0:
                self.dictionary.pop(symbol)
            delta_stock = current["amount"] - initial_amount

        return delta_cash, delta_stock, fee

    def to_dataframe(self):
        '''

        :return: Dataframe, that stores all symbol info of current position
        '''

        df = pd.DataFrame.from_dict(self.dictionary, orient="index")
        df.reset_index(inplace=True)

        return df

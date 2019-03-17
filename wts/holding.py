import pandas as pd


class Holding():
    def __init__(self, cash=0.0, stock=0.0, margin=0.0):
        total = cash + stock + margin
        self.dictionary = dict(cash=cash, stock=stock,
                               margin=margin, total=total)

    def update_holding(self, detal_amount, fee):
        self.dictionary["cash"] -= (detal_amount + fee)
        self.dictionary["stock"] += detal_amount
        self.dictionary['total'] -= fee

    def to_dataframe(self):
        '''

        :return: Dataframe, that stores all symbol info of current position
        '''

        df = pd.DataFrame.from_dict(self.dictionary, orient="index")
        return df.transpose()

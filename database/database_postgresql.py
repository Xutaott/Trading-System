from database.database_sqlite import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

if __name__ == "__main__":
    print('Using PostgreSQL as database')

    engine = create_engine('postgresql://chenxutao:@localhost/chinesestock_pg')
    # DatabaseTable.metadata.drop_all(engine)  # Drop Table to restart
    DatabaseTable.metadata.create_all(engine)
    DatabaseTable.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    load_stock_basic(session)
    load_stock_bar(session, START_DATE, END_DATE)
    load_stock_moneyflow(session, START_DATE, END_DATE)
    load_stock_technical(session, START_DATE, END_DATE)
    load_index_basic(session)
    load_index_bar(session, START_DATE, END_DATE)
    load_index_technical(session, START_DATE, END_DATE)

'''
1. database_sqlite.py:
    Include all necessary class and method to retrieve data from tushare
    and write it into sqlite database
2. databse_postgresql:
    Write data into postgresql database, optimal for computing efficiency
3. inter_database:
    Generate a inter database, in which stocks data are stored in matrix form.
    For in-sample backtesting purpose, end_date is 2 year ago
4. daily_update:
    Update the original database for every trading date after 18:00
5. monthly_update:
    Update the inter database at the first date of every month
'''

from datetime import datetime

import numpy as np
import pandas as pd
import pyodbc

import helper
import dataloader

import stock_analysis
from stock_analysis.utils import group_stocks, describe_group

fromdate = '2020-01-01'
until = datetime.today().strftime('%Y-%m-%d')

scrips = pd.read_csv('myPortfolio.csv')
scrips = scrips.dropna(how='all', axis=1)

reader = stock_analysis.StockReader(fromdate, until)

(PIDILITIND, JUBLINGREA, AMBER, METROPOLIS, SUPRAJIT, SONACOMS, FEDERALBNK,
 HDFCBANK, NEWGEN, NATCOPHARM, LTTS, TATASTEEL, MARICO, HCLTECH, DEVYANI,
 MPHASIS, LTIM, ASHOKLEY, DATAPATTNS, ITC, AUBANK, COFORGE, TECHM,
 NIFTY) = (reader.get_ticker_data(ticker) for ticker in [
     'PIDILITIND.NS', 'JUBLINGREA.NS', 'AMBER.NS', 'METROPOLIS.NS',
     'SUPRAJIT.NS', 'SONACOMS.NS', 'FEDERALBNK.NS', 'HDFCBANK.NS', 'NEWGEN.NS',
     'NATCOPHARM.NS', 'LTTS.NS', 'TATASTEEL.NS', 'MARICO.NS', 'HCLTECH.NS',
     'DEVYANI.NS', 'MPHASIS.NS', 'LTIM.NS', 'ASHOKLEY.NS', 'DATAPATTNS.NS',
     'ITC.NS', 'AUBANK.NS', 'COFORGE.NS', 'TECHM.NS', '^NSEI'
 ])

mystocks_nifty = group_stocks({
    scrip: eval(scrip)
    for scrip in pd.concat([scrips['Scrip'],
                            pd.Series(['NIFTY'])])
})


# Extract from SQL server
def get_data_from_sql(query):
    return helper.get_dataframe_from_sqlserver_query(query=query)


def etl_Data():
    toload = (mystocks_nifty
            .reset_index()[['date', 'name', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
            .assign(isIndex = lambda _df: _df['name']=='NIFTY')
                )
    
    dataloader.full_load(df=toload, tbl="stocks", hasindex=False, custom = {"id": "INT PRIMARY KEY",
                                                                                  "date": "DATE"
          })
    dataloader.full_load(df=scrips.sort_values(by='Scrip'), tbl="scrips", hasindex=False, custom = {"id": "INT PRIMARY KEY"
          })
    
    print("Done: Loaded all the tables")
    print()
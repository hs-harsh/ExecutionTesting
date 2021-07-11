import zipfile
import os, re
import pandas as pd
from os import listdir
from os.path import isfile, join
import patoolib
from shutil import copy2
import pyodbc
from pathlib import Path

def getValues(query, db):
    conn = 1
    try:
        if(db == 0):
            conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=10.230.64.2;DATABASE=Estee;UID=appsupport;PWD=estee123')            
        elif(db == 1):
            conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=192.168.155.11;DATABASE=Estee;UID=appsupport;PWD=estee123')
        elif(db == 2):
            conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=192.168.10.33;DATABASE=EsteeGlobalMaster;UID=appsupport;PWD=estee123')
        elif(db == 3):
            conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=191.191.191.151;DATABASE=Dev_Estee_NSE;UID=appsupport;PWD=estee123')            
    except:
        print("Error while opening DB conneciton, getTradeDF")
        return None
    df = pd.read_sql(query,conn)
    conn.close()
    return df

dates=['20210517', '20210506', '20210518', '20210510', '20210519', '20210526', '20210503', '20210514', '20210525', '20210527', '20210512', '20210520', '20210511', '20210507', '20210528', '20210521', '20210505', '20210524', '20210513', '20210531', '20210504']
dates.sort()
dates = dates[:3]

for i in range(0,len(dates)-1):
    date = dates[i]
    DataLogger = pd.read_csv("DataLogger_"+date+".csv")
    TradeDf = pd.read_csv("TradeLogger_"+date+".csv")
    NetTradeDf = TradeDf.groupby('EsteeID', as_index=False).agg({
            'Creation_Date':'first',
            'Buy_Sell_Indicator':'mean',
            'Traded_Quantity' : 'sum'})




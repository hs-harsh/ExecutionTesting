import zipfile
import os, re
import pandas as pd
from os import listdir
from os.path import isfile, join
import patoolib
from shutil import copy2
import pyodbc

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
    
def collect_files_from_logs(Output_dir,strategy,file_keys):
    base = "./Input"
    for month in os.listdir(base):
        if len(month)<3:
            for day in os.listdir(base + "/"+month):
                path = base + "/"+month+"/"+day
                
                for file in os.listdir(path+"/"+strategy):
                    for file_key in file_keys:
                        if (file_key) in (set(file.split("."))):
                            print(path,file)
                            copy2(path+"/"+strategy+"/"+file,Output_dir)
  
def get_esteeid_mapnum(file,base_dir="None",reverse=True):
    if('DataMM_Log' in file.split(".")):
        esteeid_mapnum = {}
        filepath = base_dir+str(file)
        my_file = open(filepath,'r')
        pattern = re.compile(r'(<\w+>)')
        for line in my_file.readlines() :
            if re.search(r'@SecId',line) :
                # print(line)
                myreg = re.findall(pattern,line)
                # print(myreg[0][1:-1])
                esteeid_mapnum[myreg[0][1:-1]] = myreg[1][1:-1]
        if reverse:
            esteeid_mapnum = {value:key for key, value in esteeid_mapnum.items()}
        return esteeid_mapnum
    
 
# collect_files_from_logs('./Output','8382',['DataLogger','DataMM_Log'])
# path_to_zip_file = './Output/'
# for f in files:
#     # patoolib.extract_archive('./Output/'+f, outdir='./Output/Unzip')
base_dir = './Output/'
files = listdir(base_dir)
dates = list(set([file.split('.')[0] for file in files[:4]]))
query ="select  EsteeID, UnderlyingTicker as Ticker from Strat_Security_Sector"
secMapdf = getValues(query, 1)
Case = 1

TradeLots = pd.DataFrame(columns = ['Ticker','NetQty','Date'])
for i in range(0,len(dates)-1):
    date = dates[i]
    print(date)
    esteeid_mapnum = get_esteeid_mapnum(date+".8382.DataMM_Log.txt0",base_dir,reverse=True)
    esteeMap = pd.DataFrame(esteeid_mapnum.items())
    esteeMap.columns = ['MappingNumber', 'EsteeID']
    esteeMap = pd.merge(esteeMap, secMapdf, how="inner",on=["EsteeID"])

    DataLogger = pd.read_csv(base_dir+date+".8382.DataLogger.txt0")
    relcol = ['MappingNumber',' BP1', ' BP2', ' BP3',
              ' AP1', ' AP2', ' AP3', ' BQ1', ' BQ2', ' BQ3', ' AQ1', ' AQ2', ' AQ3',' LTP',]
    DataLogger = DataLogger[relcol]
    priceCol = [' BP1', ' BP2', ' BP3',' AP1', ' AP2', ' AP3',' LTP',]
    DataLogger[priceCol] = DataLogger[priceCol]/10000
    DataLogger = DataLogger.astype({"MappingNumber": int})
    esteeMap = esteeMap.astype({"MappingNumber": int})
    DataLogger = pd.merge(esteeMap, DataLogger, how="right",on=["MappingNumber"])
    
    if(Case==1):
        DataLogger['NetQty'] = (DataLogger[' AQ1'] +DataLogger[' BQ1'])/2 
    DataLoggerSummary = DataLogger.groupby('Ticker', as_index=False).agg({
            'NetQty':'mean',})
    DataLoggerSummary['Date'] = date
    TradeLots = pd.concat((TradeLots, DataLoggerSummary), axis=0)
    
    query = "select Creation_Date,Estee_ID as EsteeID,Buy_Sell_Indicator,Traded_Quantity,Traded_Price,Status,TradeTime  from vwalltrades where Status in ('Executed','Partially Filled') and Creation_Date='"+str(dates[i+1])+"' and ApplyTC=1"
    TradeDf = getValues(query, 1)
    TradeDfSummary = TradeDf.groupby('EsteeID', as_index=False).agg({
            'Traded_Quantity':'sum','Buy_Sell_Indicator':'mean'})
    



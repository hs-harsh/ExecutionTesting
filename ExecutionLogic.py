import pandas as pd

dates=['20210517', '20210506', '20210518', '20210510', '20210519', '20210526', '20210503', '20210514', '20210525', '20210527', '20210512', '20210520', '20210511', '20210507', '20210528', '20210521', '20210505', '20210524', '20210513', '20210531', '20210504']
dates.sort()
dates = dates[:3]

for i in range(0,len(dates)-1):
    date = dates[i]
    DataLogger = pd.read_csv("DataLogger/DataLogger_"+date+".csv")
    TradeDf = pd.read_csv("TradeLogger/TradeLogger_"+date+".csv")
    DataLogger = DataLogger.drop(['Unnamed: 0'], axis = 1)
    TradeDf = TradeDf.drop(['Unnamed: 0'], axis = 1)

    TradeDf = TradeDf.sort_values(by=['EsteeID', 'TradeTime'],ascending=True)
    DataLogger = DataLogger.sort_values(by=['EsteeID', ' CurrTimeInMicroSeconds'],ascending=True)
    
    # A1/86400000000 + DATE(1970,1,1)
    
    NetTradeDf = TradeDf.groupby('EsteeID', as_index=False).agg({
            'Creation_Date':'first',
            'Buy_Sell_Indicator':'mean',
            'Traded_Quantity' : 'sum'})




import time
import requests
import pandas as pd
import datetime as dt
import os

previousDay = dt.datetime.now()
fn ="{0}-{1}-{2}-bithumb-btc-orderbook.csv".format(previousDay.year, previousDay.month, previousDay.day)

while(1):
    
    book = {}
    response = requests.get('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=5')
    book = response.json()

    data = book['data']

    bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric,errors='ignore')
    bids.sort_values('price', ascending=False, inplace=True)
    bids = bids.reset_index(); del bids['index']
    bids['type'] = 0
    
    asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric,errors='ignore')
    asks.sort_values('price', ascending=True, inplace=True)
    asks['type'] = 1 

    df = pd.concat([bids,asks], ignore_index=True)
    
    timestamp = dt.datetime.now()
    req_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    df['quantity'] = df['quantity'].round(decimals=4)
    df['timestamp'] = req_timestamp

    if timestamp.day != previousDay.day:
        previousDay = timestamp
        fn ="{0}-{1}-{2}-bithumb-btc-orderbook.csv".format(timestamp.year, timestamp.month, timestamp.day)

    print (df)
    print ("\n")

    should_write_header = os.path.exists(fn)
    if should_write_header == False:
        df.to_csv(fn, index=False, header=True, mode = 'a')
    else:
        df.to_csv(fn, index=False, header=False, mode = 'a')
    
    time.sleep(3.9)

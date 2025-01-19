import pandas as pd
import requests
import re
import time
import json
from datetime import datetime, timedelta
import streamlit as st

API_KEY= st.secrets["birdeye_api"]
with open('solana.txt', 'r') as f:
        solana_tickers = json.load(f)

with open('ethereum.txt', 'r') as f:
    ethereum_tickers = json.load(f)

def extract_tickers(message):
    return re.findall(r"\$([A-Za-z0-9]+)", message)

def filter_valid_tickers(row):
    tickers = extract_tickers(row["message"])
    valid = [ticker for ticker in tickers if (ticker.lower() in solana_tickers.keys() or ticker.lower() in ethereum_tickers.keys())]
    return valid if valid else None

def extract_prices(row, timeframe):
    ticker = row['valid_tickers'].lower()
    date_obj = datetime.fromisoformat(row['date'])
    value, unit = timeframe.split()
    value = int(value)
    if unit == 'hr':
        date_obj =  date_obj + timedelta(hours=value)
    elif unit == 'd':
        date_obj =  date_obj + timedelta(days=value)
    elif unit == 'w':
        date_obj =  date_obj + timedelta(weeks=value)
    elif unit == 'm':
        date_obj =  date_obj + timedelta(days=value * 30)
    unix_time = date_obj.timestamp()
    if unix_time > time.time(): return None
    address = ''
    chain='solana'
    if ticker.lower() in solana_tickers.keys():
        address = solana_tickers[ticker]
    else:
        address = ethereum_tickers[ticker]
        chain = 'ethereum'
        return None

    url = f"https://public-api.birdeye.so/defi/historical_price_unix?address={address}&unixtime={unix_time}"
    headers = {
        "accept": "application/json",
        "X-API-KEY": API_KEY,
        "x-chain": chain,
    }
    response = requests.get(url, headers=headers)
    response = response.json()
    if response['data'] and response['data']['value']:
        return response['data']['value']
    return None

async def create_df_prices(group_name : str):
    message_df = pd.read_csv(f"./data/messages_{group_name}.csv")
    message_df.dropna(inplace=True)
    message_df["valid_tickers"] = message_df.apply(filter_valid_tickers, axis=1)
    df = message_df.dropna(subset=["valid_tickers"]).reset_index(drop=True)

    df = df.explode("valid_tickers", ignore_index=True) # to seperate each valid ticker in seperate row

    timeframes = ['1 hr', '6 hr', '24 hr', '3 d', '7 d', '2 w', '1 m']
    
    df['price'] = df.apply(lambda row: extract_prices(row, '0 minutes'), axis=1)
    for timeframe in timeframes:
        df[f'price_{timeframe}']=df.apply(lambda row: extract_prices(row, timeframe), axis=1)
    
    for timeframe in timeframes:
        df[f'price_{timeframe}'] = ((df[f'price_{timeframe}']-df['price'])/df['price'])*100
    
    df.fillna(0, inplace=True)
    
    melted_df = df.melt(
        id_vars=['sender_id', 'valid_tickers'],
        value_vars=[f'price_{tf}' for tf in timeframes],
        var_name='timeframe',
        value_name='price_change'
    )
    melted_df['timeframe'] = melted_df['timeframe'].str.replace('price_', '')

    aggregated_df = melted_df.groupby(['sender_id', 'valid_tickers', 'timeframe'], as_index=False).mean()

    return aggregated_df
    
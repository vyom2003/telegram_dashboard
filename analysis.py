import pandas as pd
import requests
import re
import time
import json
from datetime import datetime, timedelta
import numpy as np
import streamlit as st
from database import insert_records, clear_group

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
    date_obj = datetime.fromisoformat(row['date'].isoformat())
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
    unix_time = int(date_obj.timestamp())
    if unix_time > time.time():
        return None
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

def create_df_prices(message_df : pd.DataFrame, group_name: str, refresh_flag: bool) -> pd.DataFrame:
    if(refresh_flag):
        clear_group(group_name)
    message_df.dropna(inplace=True)
    message_df["valid_tickers"] = message_df.apply(filter_valid_tickers, axis=1)
    df = message_df.dropna(subset=["valid_tickers"]).reset_index(drop=True)

    df = df.explode("valid_tickers", ignore_index=True) # to seperate each valid ticker in seperate row

    timeframes = ['1 hr', '6 hr', '24 hr', '3 d', '7 d', '2 w', '1 m']
    if(len(df)==0):
        return pd.DataFrame({})
    df['price'] = df.apply(lambda row: extract_prices(row, '0 minutes'), axis=1)
    for timeframe in timeframes:
        df[f'price_{timeframe}']=df.apply(lambda row: extract_prices(row, timeframe), axis=1)
    df.fillna(0, inplace=True)
    df["group_name"] = group_name
    insert_records(df, group_name)

def aggregate_df(df : pd.DataFrame, timeframe_filter, percentage_change_filter, whitelisted_symbols:str, blacklisted_symbols:str):
    timeframes = ['1hr', '6hr', '24hr', '3d', '7d', '2w', '1m']

    whitelisted_symbols = whitelisted_symbols.split(",")
    whitelisted_symbols = [w.strip().upper() for w in whitelisted_symbols]

    blacklisted_symbols = blacklisted_symbols.split(",")
    blacklisted_symbols = [b.strip().upper() for b in blacklisted_symbols]

    for timeframe in timeframes:
        df[f'price_{timeframe}'] = np.where(df[f'price_{timeframe}'] > 0, 
            ((df[f'price_{timeframe}'] - df['price']) / df['price']) * 100, 
            0)
        df = df[df[f"price_{timeframe_filter}"]>=percentage_change_filter]
    
    df.fillna(0, inplace=True)
    print(df.keys())
    melted_df = df.melt(
        id_vars=['sender_id', 'valid_tickers', 'date'],  # Include 'date' as an id_var
        value_vars=[f'price_{tf}' for tf in timeframes],
        var_name='timeframe',
        value_name='price_change'
    ).assign(valid_tickers=lambda x: x['valid_tickers'] + "_" + x['date'].astype(str))  # Modify valid_tickers

    print(melted_df.keys())
    melted_df['timeframe'] = melted_df['timeframe'].str.replace('price_', '')
    melted_df["valid_tickers"] = melted_df["valid_tickers"].str.upper()
    aggregated_df = melted_df.groupby(['sender_id', 'valid_tickers', 'timeframe'], as_index=False).mean()

    
    if len(whitelisted_symbols)>1 or whitelisted_symbols[0]!="":
        aggregated_df = aggregated_df.loc[aggregated_df["valid_tickers"].isin(whitelisted_symbols)]
    aggregated_df = aggregated_df.loc[~aggregated_df["valid_tickers"].isin(blacklisted_symbols)]
    return aggregated_df
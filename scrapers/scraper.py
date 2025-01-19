import asyncio
from telethon import TelegramClient, events
import pandas as pd
import json
import re
import streamlit as st

api_id = st.secrets["telegram_api"]
api_hash = st.secrets["telegram_hash"]
phone_number= st.secrets["phone_no"]

with open('solana.txt','r') as f:
    solana_tickers = json.load(f)
with open('ethereum.txt','r') as f:
    ethereum_tickers = json.load(f)

client = TelegramClient('session_name', api_id, api_hash)

async def connect_client():
    await client.start(phone=phone_number)
    print("Client connected")

def extract_tickers(message):
    return re.findall(r"\$([A-Za-z0-9]+)", message)

def filter_valid_tickers(row):
    tickers = extract_tickers(row["message"])
    valid = [ticker for ticker in tickers if (ticker.lower() in solana_tickers.keys() or ticker.lower() in ethereum_tickers.keys())]
    return valid if valid else None

def save_message_to_csv(message_data, file_name='messages.csv'):
    try:
        try:
            existing_data = pd.read_csv(file_name)
            new_data = pd.DataFrame([message_data])
            updated_data = pd.concat([existing_data, new_data], ignore_index=True)
        except FileNotFoundError:
            updated_data = pd.DataFrame([message_data])
        updated_data["valid_tickers"] = updated_data.apply(filter_valid_tickers, axis=1)
        updated_data = updated_data.dropna(subset=["valid_tickers"]).reset_index(drop=True)
        updated_data.to_csv(file_name, index=False)
        print(f"Message saved to {file_name}")
    except Exception as e:
        print(f"Error saving message to CSV: {e}")

@client.on(events.NewMessage(chats='https://t.me/+RUCws77zAnc1ZmU9'))
async def handler(event):
    message = event.message
    if message.message:
        message_data = {
            'date': message.date,
            'sender_id': message.sender_id,
            'message': message.message
        }
        print(f"New message received: {message_data}")
        save_message_to_csv(message_data)

async def main():
    await connect_client()
    print("Listening for new messages...")
    await client.run_until_disconnected()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

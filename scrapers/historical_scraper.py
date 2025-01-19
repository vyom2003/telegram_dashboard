from telethon import TelegramClient
import pandas as pd
import streamlit as st

async def scrape_messages(entity, limit=500):
    api_id = st.secrets["telegram_api"]
    api_hash = st.secrets["telegram_hash"]
    
    async with TelegramClient('session_name', api_id, api_hash) as client:
        messages = []
        try:
            # Get the entity (channel/group)
            chat = await client.get_entity(entity)
            
            # Iterate through messages
            async for message in client.iter_messages(chat, limit=limit):
                if message.message is not None:
                    messages.append({
                        'date': message.date,
                        'message': message.text,
                        'sender_id': message.sender_id if message.sender else None
                    })
                
        except Exception as e:
            print(f"Error scraping messages: {e}")
            return pd.DataFrame()

    return pd.DataFrame(messages)
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import json
import seaborn as sns
import os
from scrapers.historical_scraper import scrape_messages
from analysis import create_df_prices, aggregate_df
import asyncio
import multiprocessing as mp
from database import clear_group, scrape_data

if not os.path.exists('data'):
    os.makedirs('data')

def load_groups():
    try:
        with open('data/groups.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        st.error("Error reading groups file. File might be corrupted.")
        return {}

def save_groups(groups):
    with open('data/groups.json', 'w') as f:
        json.dump(groups, f, indent=4)

async def fetch_data_group(group_link, limit = 10):
    df = await scrape_messages(group_link, limit=limit)
    return df

def fetch_data(group_name, group_link):
    try:
        print(f"Starting to fetch messages for {group_name}")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        message_df = loop.run_until_complete(fetch_data_group(group_link, limit=10000))
        print(f"Fetched {len(message_df)} for {group_name}")
        refresh_flag = False
        process = mp.Process(target=create_df_prices, args=(message_df, group_name, refresh_flag))
        process.start()

    except Exception as e:
        print(e)

def refresh(group_name):
    try:
        print(f"Starting to fetch messages for {group_name}")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        message_df = loop.run_until_complete(fetch_data_group( st.session_state["groups"][group_name], limit=10000))

        print(f"Fetched {len(message_df)} for {group_name}")
        refresh_flag = True
        process = mp.Process(target=create_df_prices, args=(message_df, group_name, refresh_flag))
        process.start()

    except Exception as e:
        print(e)

def display_messages(group_name, message_count):   
    print(f"Fetching {message_count} messages from {group_name}")
    try:
        df = scrape_data(grp_name=group_name, number_of_messages=message_count)
        
        if not df.empty:
            st.write(f"Showing last {message_count} messages for group: {group_name}")
            st.dataframe(df, use_container_width=True)
        else:
            st.error("No messages found or error occurred during scraping")
        return df
    except Exception as e:
        st.error(f"Error displaying messages: {str(e)}")

def handle_group_selection():
    selection = st.session_state.group_selector
    st.session_state["current_selection"] = selection

if __name__ == "__main__":
    if "groups" not in st.session_state:
        st.session_state["groups"] = load_groups()

    if "current_selection" not in st.session_state:
        st.session_state["current_selection"] = "None"

    st.sidebar.header("Manage Groups")

    st.sidebar.selectbox(
        "Select a group",
        options=["None"] + list(st.session_state["groups"].keys()),
        key="group_selector",
        index=(["None"] + list(st.session_state["groups"].keys())).index(st.session_state["current_selection"]),
        on_change=handle_group_selection
    )

    with st.sidebar.expander("Add New Group"):
        new_group_name = st.text_input("Group Name")
        new_group_link = st.text_input("Join Link")
        
        if st.button("Add Group"):
            if new_group_name and new_group_link:
                if new_group_name not in st.session_state["groups"]:
                    st.session_state["groups"][new_group_name] = new_group_link
                    save_groups(st.session_state["groups"])
                    st.session_state["current_selection"] = new_group_name
                    fetch_data(new_group_name,new_group_link)
                    st.rerun()
                else:
                    st.error("Group name already exists.")
            else:
                st.error("Please provide both group name and join link.")

    if st.session_state["current_selection"] != "None":
        if st.sidebar.button("Delete Selected Group"):
            del st.session_state["groups"][st.session_state["current_selection"]]
            clear_group(st.session_state["current_selection"])
            save_groups(st.session_state["groups"])
            st.session_state["current_selection"] = "None"
            st.rerun()


    st.title("Telegram Message Viewer")

    if st.session_state["current_selection"] == "None":
        st.info("Please select a group from the sidebar to view messages.")
    else:
        if st.button("Refresh Group"):
            refresh(st.session_state["current_selection"])
        st.subheader(f"Messages for {st.session_state['current_selection']}")
        timeframe_filter = st.selectbox("Select Timeframe", ['1hr', '6hr', '24hr', '3d', '7d', '2w', '1m'])

        # Text input for percentage change filter
        percentage_change_filter = st.text_input("Enter Percentage Change Threshold", "0")

        # Convert percentage_change_filter to float
        try:
            percentage_change_filter = float(percentage_change_filter)
        except ValueError:
            st.warning("Please enter a valid number for percentage change.")
            percentage_change_filter = 0.0  # Default value in case of invalid input
        message_count = st.number_input(
            "Number of messages for which graph needs to be plotted", min_value=1, max_value=5000, value=500
        )
        whitelisted_symbols = st.text_input(
            "Symbols to display (comma-separated, leave blank for all)"
        )
        blacklisted_symbols = st.text_input(
            "Symbols to ban (comma-separated)"
        )

        if st.button("Plot Graph"):
            with st.spinner('Wait for it...'):
                df = display_messages(st.session_state["current_selection"], message_count)
            if not df.empty:
                with st.spinner('Wait for graphs...'):
                    st.title("Percent Change Heatmaps by Sender ID")
                    aggregated_df = aggregate_df(df, timeframe_filter, percentage_change_filter, whitelisted_symbols, blacklisted_symbols)
                    timeframes_order = ['1hr', '6hr', '24hr', '3d', '7d', '2w', '1m']
                    aggregated_df['timeframe'] = pd.Categorical(aggregated_df['timeframe'], categories=timeframes_order, ordered=True)
                    for sender_id, group in aggregated_df.groupby('sender_id'):
                        st.subheader(f"Percent Change Heatmap for Sender ID: {sender_id}")
                        
                        # Pivot the data for heatmap
                        heatmap_data = group.pivot(index='valid_tickers', columns='timeframe', values='price_change')
                        heatmap_data = heatmap_data.reindex(columns=timeframes_order)
                        # Create the heatmap
                        plt.figure(figsize=(8, len(heatmap_data)/4))
                        sns.heatmap(heatmap_data, annot=True, cmap='coolwarm', fmt='.2f', cbar_kws={'label': '% Change'})
                        plt.title(f'Percent Change Heatmap for Sender ID: {sender_id}')
                        plt.xlabel('Timeframe')
                        plt.ylabel('Ticker')
                        
                        st.pyplot(plt)
                        plt.close()

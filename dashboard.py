import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import json
import seaborn as sns
import os
from scrapers.historical_scraper import scrape_messages
from analysis import create_df_prices
import asyncio

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

if "groups" not in st.session_state:
    st.session_state["groups"] = load_groups()

if "current_selection" not in st.session_state:
    st.session_state["current_selection"] = "None"

async def display_messages(group_name, message_count):   
    print(f"Fetching {message_count} messages from {group_name}")
    try:
        df = await scrape_messages(st.session_state["groups"][group_name], limit=message_count)
        df.to_csv(f"data/messages_{group_name}.csv", index=False)
        
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

st.sidebar.header("Manage Groups")

group_options = ["None"] + list(st.session_state["groups"].keys())
st.sidebar.selectbox(
    "Select a group",
    options=group_options,
    key="group_selector",
    index=group_options.index(st.session_state["current_selection"]),
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
                st.rerun()
            else:
                st.error("Group name already exists.")
        else:
            st.error("Please provide both group name and join link.")

if st.session_state["current_selection"] != "None":
    if st.sidebar.button("Delete Selected Group"):
        del st.session_state["groups"][st.session_state["current_selection"]]
        save_groups(st.session_state["groups"])
        st.session_state["current_selection"] = "None"
        st.rerun()

st.title("Telegram Message Viewer")

if st.session_state["current_selection"] == "None":
    st.info("Please select a group from the sidebar to view messages.")
else:
    st.subheader(f"Messages for {st.session_state['current_selection']}")
    message_count = st.number_input(
        "Number of messages to display", min_value=1, value=500
    )

    if st.button("Fetch Messages"):
        with st.spinner('Wait for it...'):
            df = asyncio.run(display_messages(st.session_state["current_selection"], message_count))
        if not df.empty:
            with st.spinner('Wait for graphs...'):
                st.title("Percent Change Heatmaps by Sender ID")
                aggregated_df = asyncio.run(create_df_prices(st.session_state["current_selection"]))
                timeframes_order = ['1 hr', '6 hr', '24 hr', '3 d', '7 d', '2 w', '1 m']
                aggregated_df['timeframe'] = pd.Categorical(aggregated_df['timeframe'], categories=timeframes_order, ordered=True)
                for sender_id, group in aggregated_df.groupby('sender_id'):
                    st.subheader(f"Percent Change Heatmap for Sender ID: {sender_id}")
                    
                    # Pivot the data for heatmap
                    heatmap_data = group.pivot(index='valid_tickers', columns='timeframe', values='price_change')
                    heatmap_data = heatmap_data.reindex(columns=timeframes_order)
                    # Create the heatmap
                    plt.figure(figsize=(12, max(6, len(heatmap_data) / 2)))
                    sns.heatmap(heatmap_data, annot=True, cmap='coolwarm', fmt='.2f', cbar_kws={'label': '% Change'})
                    plt.title(f'Percent Change Heatmap for Sender ID: {sender_id}')
                    plt.xlabel('Timeframe')
                    plt.ylabel('Ticker')
                    plt.show()
                    
                    # Show the heatmap in Streamlit
                    st.pyplot(plt)
                    plt.close()  # Close the plot to avoid overlapping in Streamlit

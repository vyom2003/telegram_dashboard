import mysql.connector
import pandas as pd
import streamlit as st

def insert_records(df: pd.DataFrame, group_name: str):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password=st.secrets["sql_password"],
        database="neobase"
    )
    print(f"Inserting {len(df)} records for {group_name}")
    
    # Ensure datetime columns are converted to string format for MySQL
    if 'date' in df.columns:
        df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    cursor = connection.cursor()
    query = """
        INSERT INTO messages 
        (date, message, sender_id, valid_tickers, price, price_1hr, price_6hr, price_24hr, price_3d, price_7d, price_2w, price_1m, group_name) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Convert DataFrame to list of tuples
    data_tuples = list(df.itertuples(index=False, name=None))

    try:
        cursor.executemany(query, data_tuples)
        connection.commit()
        print(f"Successfully inserted {len(df)} records for {group_name}")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()
    
def clear_group(group_name: str):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password=st.secrets["sql_password"],
        database="neobase"
    )
    cursor = connection.cursor()
    query = f"""
        DELETE FROM messages
        WHERE group_name = "{group_name}" 
    """
    try:
        cursor.execute(query)
        connection.commit()
        print(f"Successfully deleted records for {group_name}")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()

def scrape_data(grp_name : str, number_of_messages : int):
    print("scraping")
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password=st.secrets["sql_password"],
        database="neobase"
    )
    cursor = connection.cursor()

    query = f"""
        SELECT m.*
        FROM messages m
        JOIN (
            SELECT DISTINCT date 
            FROM messages 
            WHERE group_name = '{grp_name}'
            ORDER BY date DESC 
            LIMIT {number_of_messages}
        ) sub ON m.date = sub.date
        WHERE m.group_name = '{grp_name}';
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=column_names)
        return df
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()
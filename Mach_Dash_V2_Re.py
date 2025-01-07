#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 20:54:44 2025

@author: charlesbeck
"""

import streamlit as st
import requests
import pandas as pd

# Set page configuration
st.set_page_config(
    page_title="Tristero's Mach Exchange",  # Sets the headline/title in the browser tab
    page_icon=":rocket:",           # Optional: Adds an icon to the tab
    layout="wide"                   # Optional: Adjusts layout
)

# Add custom CSS to adjust width
st.markdown(
    """
    <style>
    /* Style the main content area */
    .main {
        max-width: 95%;
        margin: auto;
        padding-top: 0px; /* Adjust the top padding */
        background-color: black; /* Set the background color to black */
        color: white; /* Ensure text is visible on black background */
    }

    /* Custom header styling */
    header {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 60px;
        background-color: #1a1a1a; /* Slightly lighter black for header */
        border-bottom: 1px solid #333; /* Subtle border for separation */
    }
    header h1 {
        font-size: 20px;
        margin: 0;
        padding: 0;
        color: white; /* White text for header title */
    }
    </style>
    <header>
        <h1>Mach By Tristero</h1>
    </header>
    """,
    unsafe_allow_html=True,
)


st.title("Mach Exchange Statistics")

if 1 == 1:
 # Supabase credentials
    supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6a2VmdGR6Z3NldWdpanBsaHNoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMjcxMzk3NCwiZXhwIjoyMDQ4Mjg5OTc0fQ.Og46ddAeoybqUavWBAUbUoj8HJiZrfAQZi-6gRP46i4"
    
    sql_query1 = """  
    SELECT op.*
    FROM order_placed op
    INNER JOIN match_executed me
    ON op.order_uuid = me.order_uuid
    """

    sql_query2 = """
    WITH source_volume_table AS(
SELECT DISTINCT
  op.*, 
  ti.decimals as source_decimal,
  cal.id as source_id,
  cal.chain as source_chain,
  cmd.current_price::FLOAT AS source_price,
  (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
FROM order_placed op
INNER JOIN match_executed me
  ON op.order_uuid = me.order_uuid
INNER JOIN token_info ti
  ON op.source_asset = ti.address  -- Get source asset decimals
INNER JOIN coingecko_assets_list cal
  ON op.source_asset = cal.address
INNER JOIN coingecko_market_data cmd 
  ON cal.id = cmd.id
),
dest_volume_table AS(
SELECT DISTINCT
  op.*, 
  ti.decimals as dest_decimal,
  cal.id as dest_id,
  cal.chain as dest_chain,
  cmd.current_price::FLOAT AS dest_price,
  (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
FROM order_placed op
INNER JOIN match_executed me
  ON op.order_uuid = me.order_uuid
INNER JOIN token_info ti
  ON op.dest_asset = ti.address  -- Get source asset decimals
INNER JOIN coingecko_assets_list cal
  ON op.dest_asset = cal.address
INNER JOIN coingecko_market_data cmd 
  ON cal.id = cmd.id
),
overall_volume_table_2 AS(
SELECT DISTINCT
  svt.*,
  dvt.dest_id as dest_id,
  dvt.dest_chain as dest_chain,
  dvt.dest_decimal as dest_decimal,
  dvt.dest_price as dest_price,
  dvt.dest_volume as dest_volume,
  (dvt.dest_volume + svt.source_volume) as total_volume
FROM source_volume_table svt
INNER JOIN dest_volume_table dvt
  ON svt.order_uuid = dvt.order_uuid
)
    SELECT 
      TO_CHAR(
        TO_TIMESTAMP(hour_series || ':00:00', 'HH24:MI:SS'),
        'FMHH AM'  -- Format hour as "1 AM", "2 AM", etc.
      ) AS hour_of_day,
      COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume
    FROM generate_series(0, 23) AS hour_series  -- Generate hours from 0 to 23
    LEFT JOIN overall_volume_table_2 svt
      ON EXTRACT(HOUR FROM svt.block_timestamp) = hour_series  -- Match the hour of the trade to the generated hours
    GROUP BY hour_series
    ORDER BY hour_series
    """

    sql_query3 = """
    WITH source_volume_table AS(
SELECT DISTINCT
  op.*, 
  ti.decimals as source_decimal,
  cal.id as source_id,
  cal.chain as source_chain,
  cmd.current_price::FLOAT AS source_price,
  (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
FROM order_placed op
INNER JOIN match_executed me
  ON op.order_uuid = me.order_uuid
INNER JOIN token_info ti
  ON op.source_asset = ti.address  -- Get source asset decimals
INNER JOIN coingecko_assets_list cal
  ON op.source_asset = cal.address
INNER JOIN coingecko_market_data cmd 
  ON cal.id = cmd.id
),
dest_volume_table AS(
SELECT DISTINCT
  op.*, 
  ti.decimals as dest_decimal,
  cal.id as dest_id,
  cal.chain as dest_chain,
  cmd.current_price::FLOAT AS dest_price,
  (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
FROM order_placed op
INNER JOIN match_executed me
  ON op.order_uuid = me.order_uuid
INNER JOIN token_info ti
  ON op.dest_asset = ti.address  -- Get source asset decimals
INNER JOIN coingecko_assets_list cal
  ON op.dest_asset = cal.address
INNER JOIN coingecko_market_data cmd 
  ON cal.id = cmd.id
),
overall_volume_table_2 AS(
SELECT DISTINCT
  svt.*,
  dvt.dest_id as dest_id,
  dvt.dest_chain as dest_chain,
  dvt.dest_decimal as dest_decimal,
  dvt.dest_price as dest_price,
  dvt.dest_volume as dest_volume,
  (dvt.dest_volume + svt.source_volume) as total_volume
FROM source_volume_table svt
INNER JOIN dest_volume_table dvt
  ON svt.order_uuid = dvt.order_uuid
)
    SELECT 
      TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
      COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume
    FROM overall_volume_table_2 svt
    GROUP BY DATE_TRUNC('day', svt.block_timestamp)
    ORDER BY day
    """

    sql_query4 = """
    WITH source_volume_table AS(
SELECT DISTINCT
  op.*, 
  ti.decimals as source_decimal,
  cal.id as source_id,
  cal.chain as source_chain,
  cmd.current_price::FLOAT AS source_price,
  (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
FROM order_placed op
INNER JOIN match_executed me
  ON op.order_uuid = me.order_uuid
INNER JOIN token_info ti
  ON op.source_asset = ti.address  -- Get source asset decimals
INNER JOIN coingecko_assets_list cal
  ON op.source_asset = cal.address
INNER JOIN coingecko_market_data cmd 
  ON cal.id = cmd.id
),
dest_volume_table AS(
SELECT DISTINCT
  op.*, 
  ti.decimals as dest_decimal,
  cal.id as dest_id,
  cal.chain as dest_chain,
  cmd.current_price::FLOAT AS dest_price,
  (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
FROM order_placed op
INNER JOIN match_executed me
  ON op.order_uuid = me.order_uuid
INNER JOIN token_info ti
  ON op.dest_asset = ti.address  -- Get source asset decimals
INNER JOIN coingecko_assets_list cal
  ON op.dest_asset = cal.address
INNER JOIN coingecko_market_data cmd 
  ON cal.id = cmd.id
),
overall_volume_table_2 AS(
SELECT DISTINCT
  svt.*,
  dvt.dest_id as dest_id,
  dvt.dest_chain as dest_chain,
  dvt.dest_decimal as dest_decimal,
  dvt.dest_price as dest_price,
  dvt.dest_volume as dest_volume,
  (dvt.dest_volume + svt.source_volume) as total_volume
FROM source_volume_table svt
INNER JOIN dest_volume_table dvt
  ON svt.order_uuid = dvt.order_uuid
)
    SELECT 
      TO_CHAR(DATE_TRUNC('week', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS week_starting,
      COALESCE(SUM(svt.total_volume), 0) AS total_weekly_volume
    FROM overall_volume_table_2 svt
    GROUP BY DATE_TRUNC('week', svt.block_timestamp)
    ORDER BY week_starting
    """

    sql_query5 = """
    SELECT 
        TO_CHAR(
            TO_TIMESTAMP(DATE_PART('hour', op.block_timestamp) || ':00:00', 'HH24:MI:SS'),
            'FMHH:MI AM'
        ) AS hour_of_day,
        COUNT(*) AS total_trades
    FROM order_placed op
    INNER JOIN match_executed me
    ON op.order_uuid = me.order_uuid
    GROUP BY DATE_PART('hour', op.block_timestamp)
    ORDER BY DATE_PART('hour', op.block_timestamp)
    """

    sql_query6 = """
    SELECT 
        DATE(op.block_timestamp) AS trade_date,
        COUNT(*) AS total_trades
    FROM order_placed op
    INNER JOIN match_executed me
    ON op.order_uuid = me.order_uuid
    GROUP BY DATE(op.block_timestamp)
    ORDER BY trade_date
    """

    sql_query7 = """
    SELECT 
        DATE_TRUNC('week', op.block_timestamp) AS week_start_date,
        COUNT(*) AS total_trades
    FROM order_placed op
    INNER JOIN match_executed me
    ON op.order_uuid = me.order_uuid
    GROUP BY DATE_TRUNC('week', op.block_timestamp)
    ORDER BY week_start_date
    """
    
    sql_query8 = """
    SELECT
        DISTINCT address AS unique_address_count
    FROM (
        SELECT sender_address AS address
        FROM order_placed op
        INNER JOIN match_executed me
            ON op.order_uuid = me.order_uuid
        UNION
        SELECT maker_address AS address
        FROM order_placed op
        INNER JOIN match_executed me
            ON op.order_uuid = me.order_uuid
    ) AS unique_addresses
    """
    
    sql_query9 = """
    SELECT COUNT(op.order_uuid)
        FROM order_placed op
        INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
    """
    
    sql_query10 = """
    SELECT
        address,
        COUNT(order_id) AS trade_count
    FROM (
        SELECT sender_address AS address, op.order_uuid AS order_id
        FROM order_placed op
        INNER JOIN match_executed me
            ON op.order_uuid = me.order_uuid
        UNION ALL
        SELECT maker_address AS address, op.order_uuid AS order_id
        FROM order_placed op
        INNER JOIN match_executed me
            ON op.order_uuid = me.order_uuid
    ) AS all_trades
    GROUP BY address
    ORDER BY trade_count DESC
    LIMIT 50
    """
    
    sql_query11 = """
    WITH source_volume_table AS (
    SELECT DISTINCT
        op.order_uuid, 
        op.source_quantity, 
        op.source_asset,
        op.sender_address,  -- Explicitly include sender_address
        ti.decimals AS source_decimal,
        cal.id AS source_id,
        cal.chain AS source_chain,
        cmd.current_price::FLOAT AS source_price,
        (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
    FROM order_placed op
    INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
    INNER JOIN token_info ti
        ON op.source_asset = ti.address
    INNER JOIN coingecko_assets_list cal
        ON op.source_asset = cal.address
    INNER JOIN coingecko_market_data cmd 
        ON cal.id = cmd.id
    ),
    dest_volume_table AS (
    SELECT DISTINCT
        op.order_uuid, 
        op.dest_quantity, 
        op.dest_asset,
        me.maker_address,  -- Explicitly include maker_address
        ti.decimals AS dest_decimal,
        cal.id AS dest_id,
        cal.chain AS dest_chain,
        cmd.current_price::FLOAT AS dest_price,
        (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
    FROM order_placed op
    INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
    INNER JOIN token_info ti
        ON op.dest_asset = ti.address
    INNER JOIN coingecko_assets_list cal
        ON op.dest_asset = cal.address
    INNER JOIN coingecko_market_data cmd 
        ON cal.id = cmd.id
    ),
    overall_volume_table_2 AS (
    SELECT DISTINCT
        svt.order_uuid,
        svt.sender_address,  -- Explicitly use sender_address here
        dvt.maker_address,   -- Explicitly use maker_address here
        svt.source_volume,
        dvt.dest_volume,
        (dvt.dest_volume + svt.source_volume) AS total_volume
    FROM source_volume_table svt
    INNER JOIN dest_volume_table dvt
        ON svt.order_uuid = dvt.order_uuid
    )
    SELECT 
        address,
        COALESCE(SUM(total_volume), 0) AS total_user_volume
    FROM (
        SELECT sender_address AS address, total_volume
        FROM overall_volume_table_2
        UNION ALL
        SELECT maker_address AS address, total_volume
        FROM overall_volume_table_2
    ) AS combined_addresses
    GROUP BY address
    ORDER BY total_user_volume DESC
    LIMIT 50
    """
    
    @st.cache_data
    def execute_sql(query):
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        # Endpoint for the RPC function
        rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
        
        # Payload with the SQL query
        payload = {"query": query}
        
        # Make the POST request to the RPC function
        response = requests.post(rpc_endpoint, headers=headers, json=payload)
        
        # Handle response
        if response.status_code == 200:
            data = response.json()
            
            df = pd.DataFrame(data)
            
            print("Query executed successfully, returning DataFrame.")
            return(df)
        else:
            print("Error executing query:", response.status_code, response.json())

    # Call the function
    df_hourly_volume = execute_sql(sql_query2)
    df_daily_volume = execute_sql(sql_query3)
    df_weekly_volume = execute_sql(sql_query4)
    

    df_hourly_trades = execute_sql(sql_query5)
    df_daily_trades = execute_sql(sql_query6)
    df_weekly_trades = execute_sql(sql_query7)
    
    df_total_users =  execute_sql(sql_query8)
    df_total_trades = execute_sql(sql_query9)
    
    df_trade_address = execute_sql(sql_query10)
    df_volume_address = execute_sql(sql_query11)
    
    df_trade_address = pd.json_normalize(df_trade_address['result'])
    df_volume_address = pd.json_normalize(df_volume_address['result'])
    
    trade_count = int(pd.json_normalize(df_total_trades['result'])['count'][0])
    # Dictionary holding the DataFrames
    dfs = {
        "hourly_volume": df_hourly_volume,
        "daily_volume": df_daily_volume,
        "weekly_volume": df_weekly_volume,
        "hourly_trades": df_hourly_trades,
        "daily_trades": df_daily_trades,
        "weekly_trades": df_weekly_trades,
    }

    for key in dfs:
        dfs[key] = pd.json_normalize(dfs[key]['result'])

    # Convert date columns explicitly to datetime
    #dfs["daily_volume"]["day"] = pd.to_datetime(dfs["daily_volume"]["day"], format='%m-%d-%Y', errors='coerce')
    #dfs["weekly_volume"]["week_starting"] = pd.to_datetime(dfs["weekly_volume"]["week_starting"], format='%m-%d-%Y', errors='coerce')
    dfs["daily_trades"]["trade_date"] = pd.to_datetime(dfs["daily_trades"]["trade_date"], format='%Y-%m-%d', errors='coerce')
    dfs["weekly_trades"]["week_start_date"] = pd.to_datetime(dfs["weekly_trades"]["week_start_date"], format='%Y-%m-%dT%H:%M:%S', errors='coerce')

    # Add missing hours to hourly_trades (zero padding)
    all_hours = [f"{i}:00 AM" if i < 12 else f"{i-12}:00 PM" if i > 12 else f"{i}:00 PM" for i in range(24)]
    dfs["hourly_trades"] = dfs["hourly_trades"].set_index("hour_of_day").reindex(all_hours, fill_value=0).reset_index()
    dfs["hourly_trades"].rename(columns={"index": "hour_of_day"}, inplace=True)

    # Drop rows with invalid datetime or missing values in important columns
    for df_name in ["hourly_volume", "daily_volume", "weekly_volume", "hourly_trades", "daily_trades", "weekly_trades"]:
        # Check if the columns exist before dropping NaNs
        if "trade_date" in dfs[df_name].columns:
            dfs[df_name].dropna(subset=["trade_date"], inplace=True)
        if "week_start" in dfs[df_name].columns:
            dfs[df_name].dropna(subset=["week_start"], inplace=True)
        if "week_start_date" in dfs[df_name].columns:
            dfs[df_name].dropna(subset=["week_start_date"], inplace=True)
        if "hour_of_day" in dfs[df_name].columns:
            dfs[df_name].dropna(subset=["hour_of_day"], inplace=True)
        dfs[df_name] = dfs[df_name].reset_index(drop=True)

    # Convert columns with large numbers to float64 to handle overflow issues
    dfs["hourly_volume"]["total_hourly_volume"] = dfs["hourly_volume"]["total_hourly_volume"].astype('float64')
    dfs["daily_volume"]["total_daily_volume"] = dfs["daily_volume"]["total_daily_volume"].astype('float64')
    dfs["weekly_volume"]["total_weekly_volume"] = dfs["weekly_volume"]["total_weekly_volume"].astype('float64')


    # Ensure the 'day' column is converted to a proper datetime format
    dfs["daily_volume"]['day'] = pd.to_datetime(dfs["daily_volume"]['day'], format='%B %d, %Y')
    dfs["weekly_volume"]["week_starting"] = pd.to_datetime(dfs["weekly_volume"]["week_starting"], format='%B %d, %Y')

    # Sort the DataFrame by the 'day' column in ascending order
    dfs["daily_volume"] = dfs["daily_volume"].sort_values(by='day')
    dfs["weekly_volume"] = dfs["weekly_volume"].sort_values(by = 'week_starting')

    # Convert 'day' column back to the original string format after sorting
    dfs["daily_volume"]['day'] = dfs["daily_volume"]['day'].dt.strftime('%B %d, %Y')
    dfs["weekly_volume"]["week_starting"] = dfs["weekly_volume"]["week_starting"].dt.strftime('%B %d, %Y')
    
# Define the layout
col1, col2, col3 = st.columns(3)

total_volume = float(dfs["weekly_volume"]["total_weekly_volume"].sum())

# Box 1
with col1:
    st.metric(label="Total Volume", value=f"${total_volume:,.2f}")
    #st.line_chart(data["A"])

# Box 2
with col2:
    st.metric(label="Total  Users", value=len(df_total_users))
    #st.bar_chart(data["B"])
    
with col3:
    st.metric(label="Total Trades", value=f"{trade_count:,}")

# Additional styling for more customization (optional)
st.markdown(
    """
    <style>
    .stMetric {
        border: 1px solid #ccc;
        border-radius: 10px;
        padding: 10px;
        margin: 5px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
    
col1, col2 = st.columns(2)

with col1:
    st.write("Total Volume")
    daily_data = dfs["daily_volume"].set_index("day")["total_daily_volume"]
        
    # Convert the daily data index to datetime without time
    daily_data.index = pd.to_datetime(daily_data.index).date  # Keep only the date part

    # Compute cumulative sum
    cumulative_data = daily_data.cumsum()

    # Display the cumulative data as a line chart
    st.line_chart(cumulative_data, use_container_width=True)
    
with col2:
    
    st.write("Volume")
    daily_data = dfs["daily_volume"].set_index("day")["total_daily_volume"]
        
    # Convert the daily data index to datetime without time
    daily_data.index = pd.to_datetime(daily_data.index).date  # Keep only the date part

    st.line_chart(daily_data, use_container_width=True)
    
col1, col2 = st.columns(2)

# Initialize session state if not already present
if 'df_trade_address' not in st.session_state:
    st.session_state.df_trade_address = df_trade_address
    st.session_state.df_volume_address = df_volume_address
    st.session_state.page_trade = 0
    st.session_state.page_volume = 0

# Pagination function
@st.cache_data
def paginate_df(df, page=0, page_size=10):
    start_row = page * page_size
    end_row = start_row + page_size
    return df.iloc[start_row:end_row]

# Calculate total pages for trade and volume data
def calculate_total_pages(df, page_size=10):
    total_pages = len(df) // page_size
    if len(df) % page_size != 0:
        total_pages += 1  # Account for remaining rows if not divisible by page_size
    return total_pages

# Helper function to trigger state change
def handle_page_change(page_key, direction, total_pages):
    if direction == 'next' and st.session_state[page_key] < total_pages - 1:
        st.session_state[page_key] += 1
    elif direction == 'previous' and st.session_state[page_key] > 0:
        st.session_state[page_key] -= 1

# For 'Users With The Most Trades' section
col1, col2 = st.columns(2)

with col1:
    st.write("Users With The Most Trades")
    
    # Get the paginated DataFrame
    df_paginated_trade = paginate_df(st.session_state.df_trade_address, st.session_state.page_trade)
    
    # Display the DataFrame
    st.write(df_paginated_trade)
    
    # Calculate total pages for trade data
    total_pages_trade = calculate_total_pages(st.session_state.df_trade_address)
    
    # Pagination buttons for trade data
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        # Handle previous button
        if st.button('Previous', key='prev_trade'):
            handle_page_change('page_trade', 'previous', total_pages_trade)
            
    with col_right:
        # Handle next button
        if st.button('Next', key='next_trade'):
            handle_page_change('page_trade', 'next', total_pages_trade)

# For 'Users With The Most Volume' section
with col2:
    st.write("Users With The Most Volume")
    
    # Get the paginated DataFrame
    df_paginated_volume = paginate_df(st.session_state.df_volume_address, st.session_state.page_volume)
    
    # Display the DataFrame
    st.write(df_paginated_volume)
    
    # Calculate total pages for volume data
    total_pages_volume = calculate_total_pages(st.session_state.df_volume_address)
    
    # Pagination buttons for volume data
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        # Handle previous button
        if st.button('Previous', key='prev_volume'):
            handle_page_change('page_volume', 'previous', total_pages_volume)
            
    with col_right:
        # Handle next button
        if st.button('Next', key='next_volume'):
            handle_page_change('page_volume', 'next', total_pages_volume)

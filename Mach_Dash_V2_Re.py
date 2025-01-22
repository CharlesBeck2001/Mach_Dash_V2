#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 20:54:44 2025

@author: charlesbeck
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import altair as alt
import plotly.graph_objects as go
from datetime import datetime, timedelta

supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
supabase_key = st.secrets["supabase_key"]

st.set_page_config(
    page_title="Tristero's Mach Exchange",  # Sets the headline/title in the browser tab
    page_icon=":rocket:",           # Optional: Adds an icon to the tab
    layout="wide"                   # Optional: Adjusts layout
)

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

time_query = """
SELECT MIN(op.block_timestamp) AS oldest_time
FROM order_placed op
INNER JOIN match_executed me
ON op.order_uuid = me.order_uuid
"""

time_point = execute_sql(time_query)
time_point = pd.json_normalize(time_point['result'])

# Time range options
time_ranges = {
    "All Time": None,  # Special case for no date filter
    "Last Week": 7,
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}

day_list = [7,30,90,180]

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
        background-color: white; /* Slightly lighter black for header */
        border-bottom: 1px solid #333; /* Subtle border for separation */
    }
    header h1 {
        font-size: 20px;
        margin: 0;
        padding: 0;
        color: green; /* White text for header title */
    }
    </style>
    <header>
        <h1>Mach By Tristero</h1>
    </header>
    """,
    unsafe_allow_html=True,
)
st.title("Mach Exchange Statistics")
# Get today's date
today = datetime.now()

if "today" not in st.session_state:
    st.session_state["today"] = datetime.now()

# Use session state to track the selected range
if "selected_range" not in st.session_state:
    st.session_state["selected_range"] = "All Time"  # Default value

# Initialize session_state for start_date if not already set
if "start_date" not in st.session_state:
    today = st.session_state["today"]
    if time_ranges[st.session_state["selected_range"]] is not None:
        start_date = today - timedelta(days=time_ranges[st.session_state["selected_range"]])
        st.session_state["start_date"] = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    else:
        # If "All Time", set it to a specific point (replace with your own logic)
        st.session_state["start_date"] = time_point['oldest_time'][0]

# Function to update start_date in session_state when selection changes
# Function to update the start date
def update_start_date(selected_range):
    today = st.session_state["today"]
    if time_ranges[selected_range] is not None:
        start_date = today - timedelta(days=time_ranges[selected_range])
        st.session_state["start_date"] = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    else:
        st.session_state["start_date"] = time_point['oldest_time'][0]


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

#stats_placeholder = st.empty()

#if start_date != st.session_state['start_date']:
if 1==1:
    
    @st.cache_data
    def stats_box_maker(sd):
 # Supabase credentials
        supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
        supabase_key = st.secrets["supabase_key"]
        
        sql_query1 = f"""  
        SELECT op.order_uuid
        FROM order_placed op
        INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
        WHERE op.block_timestamp >= '{sd}'
        """
    
        sql_query2 = f"""
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
    WHERE op.block_timestamp >= '{sd}'
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
    WHERE op.block_timestamp >= '{sd}'
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
    
        sql_query3 = f"""
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
    WHERE op.block_timestamp >= '{sd}'
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
    WHERE op.block_timestamp >= '{sd}'
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
    
        sql_query4 = f"""
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
    WHERE op.block_timestamp >= '{sd}'
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
    WHERE op.block_timestamp >= '{sd}'
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
    
        sql_query5 = f"""
        SELECT 
            TO_CHAR(
                TO_TIMESTAMP(DATE_PART('hour', op.block_timestamp) || ':00:00', 'HH24:MI:SS'),
                'FMHH:MI AM'
            ) AS hour_of_day,
            COUNT(*) AS total_trades
        FROM order_placed op
        INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
        WHERE op.block_timestamp >= '{sd}'
        GROUP BY DATE_PART('hour', op.block_timestamp)
        ORDER BY DATE_PART('hour', op.block_timestamp)
        """
    
        sql_query6 = f"""
        SELECT 
            DATE(op.block_timestamp) AS trade_date,
            COUNT(*) AS total_trades
        FROM order_placed op
        INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
        WHERE op.block_timestamp >= '{sd}'
        GROUP BY DATE(op.block_timestamp)
        ORDER BY trade_date
        """
    
        sql_query7 = f"""
        SELECT 
            DATE_TRUNC('week', op.block_timestamp) AS week_start_date,
            COUNT(*) AS total_trades
        FROM order_placed op
        INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
        WHERE op.block_timestamp >= '{sd}'
        GROUP BY DATE_TRUNC('week', op.block_timestamp)
        ORDER BY week_start_date
        """
        
        sql_query8 = f"""
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
            WHERE op.block_timestamp >= '{sd}'
        ) AS unique_addresses
        """
        
        sql_query9 = f"""
        SELECT COUNT(op.order_uuid)
            FROM order_placed op
            INNER JOIN match_executed me
            ON op.order_uuid = me.order_uuid
            WHERE op.block_timestamp >= '{sd}'
        """
        
        sql_query10 = f"""
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
            WHERE op.block_timestamp >= '{sd}'
        ) AS all_trades
        GROUP BY address
        ORDER BY trade_count DESC
        LIMIT 200
        """
        
        sql_query11 = f"""
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
        WHERE op.block_timestamp >= '{sd}'
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
        WHERE op.block_timestamp >= '{sd}'
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
        LIMIT 200
        """
    
        sql_query12 = f""" 
        WITH RankedTrades AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY COUNT(order_id) DESC) AS rank,
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
            WHERE op.block_timestamp >= '{sd}'
        ) AS all_trades
        GROUP BY address
        ),
        CumulativeTrades AS (
        SELECT
            rank AS N,
            SUM(trade_count) OVER (ORDER BY rank) AS cumulative_trade_count,
            (SELECT SUM(trade_count) FROM RankedTrades) AS total_trades
        FROM RankedTrades
        WHERE rank <= 200
        )
        SELECT
            N,
            CAST(cumulative_trade_count * 100.0 / total_trades AS FLOAT) AS percentage_of_total_trades
        FROM CumulativeTrades
        ORDER BY N
        """
    
        sql_query13 = f"""
        WITH source_volume_table AS (
        SELECT DISTINCT
            op.order_uuid, 
            op.source_quantity, 
            op.source_asset,
            op.sender_address,  
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
        WHERE op.block_timestamp >= '{sd}'
        ),
        dest_volume_table AS (
            SELECT DISTINCT
                op.order_uuid, 
                op.dest_quantity, 
                op.dest_asset,
                me.maker_address,  
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
            WHERE op.block_timestamp >= '{sd}'
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.order_uuid,
                svt.sender_address,  
                dvt.maker_address,   
                svt.source_volume,
                dvt.dest_volume,
                (dvt.dest_volume + svt.source_volume) AS total_volume
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        ),
        total_volume_table AS (
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
        ),
        ranked_volume_table AS (
            SELECT 
                address,
                total_user_volume,
                RANK() OVER (ORDER BY total_user_volume DESC) AS rank
            FROM total_volume_table
        ),
        cumulative_volume_table AS (
            SELECT 
                rank,
                SUM(total_user_volume) OVER (ORDER BY rank) AS cumulative_volume,
                (SUM(total_user_volume) OVER (ORDER BY rank) * 100.0) / (SUM(total_user_volume) OVER ()) AS percentage_of_total_volume
            FROM ranked_volume_table
        )
        SELECT 
            rank AS top_n,
            percentage_of_total_volume
        FROM cumulative_volume_table
        WHERE rank <= 300
        """
    
        sql_query14 = f"""
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
      WHERE op.block_timestamp >= '{sd}'
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
    WHERE op.block_timestamp >= '{sd}'
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
    ),
    combined_address_trades AS (
      SELECT 
        sender_address AS address,
        COUNT(*) AS trades
      FROM overall_volume_table_2
      GROUP BY sender_address
    
      UNION ALL
    
      SELECT 
        filler_address AS address,
        COUNT(*) AS trades
      FROM overall_volume_table_2
      GROUP BY filler_address
    ),
    user_trade_count AS (
    SELECT 
      address,
      CAST(SUM(trades) AS INT) AS total_trades
    FROM combined_address_trades
    GROUP BY address
    ORDER BY total_trades DESC
    )
    SELECT CAST(AVG(total_trades) AS INT) AS average_trades_per_user FROM user_trade_count
        """
    
        sql_query15 = f"""
        WITH user_trade_counts AS (
            SELECT
                op.sender_address AS address,
                COUNT(op.order_uuid) AS trade_count
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            WHERE op.block_timestamp >= '{sd}'
            GROUP BY op.sender_address
        )   
        SELECT
            CAST(
                (COUNT(CASE WHEN trade_count > 1 THEN 1 END) * 100.0) / COUNT(*) AS INT
            ) AS percent_users_with_more_than_one_trade
        FROM user_trade_counts
        """
    
        sql_query16 = f"""
        WITH user_trade_counts AS (
            SELECT
                op.sender_address AS address,
                COUNT(op.order_uuid) AS trade_count
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            WHERE op.block_timestamp >= '{sd}'
            GROUP BY op.sender_address
        )   
        SELECT COUNT(*) FROM user_trade_counts
        """

        sql_query17 = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
            FROM order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        dest_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                dvt.dest_id AS dest_id,
                dvt.dest_chain AS dest_chain,
                dvt.dest_decimal AS dest_decimal,
                dvt.dest_price AS dest_price,
                dvt.dest_volume AS dest_volume,
                (dvt.dest_volume + svt.source_volume) AS total_volume
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        )
        SELECT 
            SUM(total_volume) as volume
        FROM overall_volume_table_2 svt
        """

        sql_query18 = """
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(created_at)) AS max_date
            FROM mm_order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.created_at >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.created_at < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                svt.source_volume AS total_volume
            FROM source_volume_table svt
        )
        SELECT 
            2*SUM(total_volume) AS volume
        FROM overall_volume_table_2
        """
        
        
        df_sql_timeframe = execute_sql(sql_query1)
        df_sql_timeframe = pd.json_normalize(df_sql_timeframe['result'])
        #st.write(df_sql_timeframe)
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
    
        df_trade_rank = execute_sql(sql_query12)
    
        df_volume_rank = execute_sql(sql_query13)
    
        df_average_trades = execute_sql(sql_query14)
    
        df_count_above = execute_sql(sql_query16)
    
        df_count_above = pd.json_normalize(df_count_above['result'])
    
        count_above = df_count_above['count'].iloc[0]
    
        if count_above == 0:
    
            perc_above = 0
    
        else:
        
            df_perc_above = execute_sql(sql_query15)
        
            df_perc_above = pd.json_normalize(df_perc_above['result'])
    
            perc_above = df_perc_above['percent_users_with_more_than_one_trade'].iloc[0]
    
        df_last_day_v = execute_sql(sql_query18)    
        
        df_last_day_v  = pd.json_normalize(df_last_day_v['result'])

        last_day_v = df_last_day_v['volume'].iloc[0]
        
        df_average_trades = pd.json_normalize(df_average_trades['result'])
        #st.write(df_average_trades['average_trades_per_year'])
        average_trades = df_average_trades['average_trades_per_user'].iloc[0]
    
        df_trade_address = pd.json_normalize(df_trade_address['result'])
        df_volume_address = pd.json_normalize(df_volume_address['result'])
    
        df_trade_rank = pd.json_normalize(df_trade_rank['result'])
    
        df_volume_rank = pd.json_normalize(df_volume_rank['result'])
        
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

        total_volume = float(dfs["weekly_volume"]["total_weekly_volume"].sum())
        total_users = len(df_total_users)

        
        return {
            "total_volume": total_volume,
            "total_users": total_users,
            "trade_count": trade_count,
            "average_trades": average_trades,
            "perc_above": perc_above,
            "last_day_v": last_day_v
        }

    
    def load_metrics(data):
        # Define the layout
                
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        # Box 1
        with col1:
            st.metric(label="Total Volume", value=f"${data['total_volume']:,.2f}")
            #st.line_chart(data["A"])
        # Box 2
        with col2:
            st.metric(label="Total  Users", value=data['total_users'])
            #st.bar_chart(data["B"])
            
        with col3:
            st.metric(label="Total Trades", value=f"{data['trade_count']:,}")
            total_trades = data['trade_count']
        
        with col4:
            st.metric(label="Average Trades Per User", value=data['average_trades'])
        
        with col5:
            st.metric(label="Percent of Users With More Than 1 Trade",value=data['perc_above'])

        with col6: 
            st.metric(label="Total Volume in the Previous Active Day",value = f"${data['last_day_v']:,.2f}")

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
        # Additional styling for more customization (optional)
        
if "preloaded" not in st.session_state:
    preloaded = {}
    for i in day_list:
        date = today - timedelta(days=i)
        date = date.strftime('%Y-%m-%dT%H:%M:%S')
    
        data = stats_box_maker(date)
        preloaded[i] = data
    
    date = time_point['oldest_time'][0]
    data = stats_box_maker(date)
    st.write(date)
    preloaded[0] = data

    st.session_state["preloaded"] = preloaded


selected_range = st.selectbox(
    "Select a time range:",
    list(time_ranges.keys()),
    index=0,  # Default to "All Time"
    key="range_selector",
)

# Update start_date when the selection changes
update_start_date(selected_range)
if time_ranges[selected_range] is not None:
    load_metrics(st.session_state["preloaded"][time_ranges[selected_range]])
    date = today - timedelta(days=time_ranges[selected_range])
    date = date.strftime('%Y-%m-%dT%H:%M:%S')
    
else:
    load_metrics(st.session_state["preloaded"][0])
    

st.title("Volume Analysis")

# Get today's date
#today = datetime.now()

@st.cache_data
def asset_fetch():
    asset_query = f"""
    WITH source_volume_table AS (
        SELECT DISTINCT
            op.*, 
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
            op.*, 
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
            svt.*,
            dvt.dest_id AS dest_id,
            dvt.dest_chain AS dest_chain,
            dvt.dest_decimal AS dest_decimal,
            dvt.dest_price AS dest_price,
            dvt.dest_volume AS dest_volume,
            (dvt.dest_volume + svt.source_volume) AS total_volume
        FROM source_volume_table svt
        INNER JOIN dest_volume_table dvt
            ON svt.order_uuid = dvt.order_uuid
    ),
    consolidated_volumes AS (
        SELECT
            id,
            SUM(volume) AS total_volume
        FROM (
            SELECT
                source_id AS id,
                SUM(source_volume) AS volume
            FROM overall_volume_table_2
            GROUP BY source_id
            UNION ALL
            SELECT
                dest_id AS id,
                SUM(dest_volume) AS volume
            FROM overall_volume_table_2
            GROUP BY dest_id
        ) combined
        GROUP BY id
    )
    SELECT id
    FROM consolidated_volumes
    ORDER BY total_volume DESC
    """

    asset_list = execute_sql(asset_query)
    asset_list = pd.json_normalize(asset_list['result'])['id'].tolist()
    return(asset_list)

@st.cache_data
def asset_fetch_day():
    asset_query = f"""
    WITH latest_date AS (
        SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
        FROM order_placed
    ),
    source_volume_table AS (
        SELECT DISTINCT
            op.*, 
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
        WHERE op.block_timestamp >= (
                SELECT max_date - INTERVAL '1 day'
                FROM latest_date
            )
          AND op.block_timestamp < (
                SELECT max_date
                FROM latest_date
            )
    ),
    dest_volume_table AS (
        SELECT DISTINCT
            op.*, 
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
        WHERE op.block_timestamp >= (
                SELECT max_date - INTERVAL '1 day'
                FROM latest_date
            )
          AND op.block_timestamp < (
                SELECT max_date
                FROM latest_date
            )
    ),
    overall_volume_table_2 AS (
        SELECT DISTINCT
            svt.*,
            dvt.dest_id AS dest_id,
            dvt.dest_chain AS dest_chain,
            dvt.dest_decimal AS dest_decimal,
            dvt.dest_price AS dest_price,
            dvt.dest_volume AS dest_volume,
            (dvt.dest_volume + svt.source_volume) AS total_volume
        FROM source_volume_table svt
        INNER JOIN dest_volume_table dvt
            ON svt.order_uuid = dvt.order_uuid
    ),
    consolidated_volumes AS (
        SELECT
            id,
            SUM(volume) AS total_volume
        FROM (
            SELECT
                source_id AS id,
                SUM(source_volume) AS volume
            FROM overall_volume_table_2
            GROUP BY source_id
            UNION ALL
            SELECT
                dest_id AS id,
                SUM(dest_volume) AS volume
            FROM overall_volume_table_2
            GROUP BY dest_id
        ) combined
        GROUP BY id
    )
    SELECT id
    FROM consolidated_volumes
    ORDER BY total_volume DESC
    """

    asset_query_2 = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(created_at)) AS max_date
            FROM mm_order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.created_at >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.created_at < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                svt.source_volume AS total_volume
            FROM source_volume_table svt
        ),
        consolidated_volumes AS (
        SELECT
            id,
            SUM(volume) AS total_volume
        FROM (
            SELECT
                source_id AS id,
                SUM(source_volume) AS volume
            FROM overall_volume_table_2
            GROUP BY source_id
        ) combined
        GROUP BY id
    )
    SELECT id
    FROM consolidated_volumes
    ORDER BY total_volume DESC
    """
    
    # Execute the query and process results
    asset_list = execute_sql(asset_query_2)
    asset_list = pd.json_normalize(asset_list['result'])['id'].tolist()
    return asset_list

# Function to execute query and retrieve data
@st.cache_data
def get_volume_vs_date(asset_id, sd):
    """
    Query the Supabase database to get total volume vs date for a specific asset.

    Args:
        asset_id (str): The asset ID for which to retrieve the volume data.

    Returns:
        pd.DataFrame: A DataFrame containing dates and their corresponding total volumes.
    """
    # SQL query to retrieve volume vs date for the given asset_id
    if asset_id != 'Total':
        query = f"""
        WITH source_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
                op.*, 
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
                svt.order_uuid AS order_id,
                (dvt.dest_volume + svt.source_volume) AS total_volume,
                svt.block_timestamp AS date,
                svt.source_id AS source_asset,
                dvt.dest_id AS dest_asset
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        ),
        source_volume_table_3 AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        overall_volume_table_3 AS (
            SELECT DISTINCT
                svt.order_uuid AS order_id,
                2*svt.source_volume AS total_volume,
                svt.created_at AS date,
                svt.source_id AS source_asset,
                '' AS dest_asset
            FROM source_volume_table_3 svt
        ),
        combined_volume_table AS (
            SELECT DISTINCT
                * 
            FROM overall_volume_table_2
            UNION
            SELECT DISTINCT
                * 
            FROM overall_volume_table_3
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.date), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume,
            '{asset_id}' AS asset
        FROM combined_volume_table svt
        WHERE svt.source_asset = '{asset_id}' OR svt.dest_asset = '{asset_id}'
        GROUP BY DATE_TRUNC('day', svt.date)
        ORDER BY DATE_TRUNC('day', svt.date)
        """
    else:

        query = f"""
        WITH source_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
                op.*, 
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
                svt.order_uuid AS order_id,
                (dvt.dest_volume + svt.source_volume) AS total_volume,
                svt.block_timestamp AS date,
                svt.source_id AS source_asset,
                dvt.dest_id AS dest_asset
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        ),
        source_volume_table_3 AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        overall_volume_table_3 AS (
            SELECT DISTINCT
                svt.order_uuid AS order_id,
                2*svt.source_volume AS total_volume,
                svt.created_at AS date,
                svt.source_id AS source_asset,
                '' AS dest_asset
            FROM source_volume_table_3 svt
        ),
        combined_volume_table AS (
            SELECT DISTINCT
                * 
            FROM overall_volume_table_2
            UNION
            SELECT DISTINCT
                * 
            FROM overall_volume_table_3
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.date), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume,
            '{asset_id}' AS asset
        FROM combined_volume_table svt
        GROUP BY DATE_TRUNC('day', svt.date)
        ORDER BY DATE_TRUNC('day', svt.date)
        """

    # Execute the query and return the result as a DataFrame
    return pd.json_normalize(execute_sql(query)['result'])

@st.cache_data
def get_weekly_volume_vs_date(asset_id, sd):
    """
    Query the Supabase database to get weekly averaged volume vs date for a specific asset.

    Args:
        asset_id (str): The asset ID for which to retrieve the volume data.

    Returns:
        pd.DataFrame: A DataFrame containing dates and their corresponding weekly averaged volumes.
    """
    # SQL query to retrieve weekly averaged volume vs date for the given asset_id
    if asset_id != 'Total':
        query = f"""
        WITH date_series AS (
            -- Generate a series of dates from the minimum to the maximum block timestamp
            SELECT 
                generate_series(
                    (SELECT MIN(DATE_TRUNC('day', block_timestamp)) FROM order_placed), 
                    (SELECT MAX(DATE_TRUNC('day', block_timestamp)) FROM order_placed), 
                    '1 day'::interval
                )::date AS day
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
                op.*, 
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
                svt.order_uuid AS order_id,
                (dvt.dest_volume + svt.source_volume) AS total_volume,
                svt.block_timestamp AS date,
                svt.source_id AS source_id,
                dvt.dest_id AS dest_id
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        ),
        source_volume_table_3 AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        overall_volume_table_3 AS (
            SELECT DISTINCT
                svt.order_uuid AS order_id,
                2*svt.source_volume AS total_volume,
                svt.created_at AS date,
                svt.source_id AS source_id,
                '' AS dest_id
            FROM source_volume_table_3 svt
        ),
        combined_volume_table AS (
            SELECT DISTINCT
                * 
            FROM overall_volume_table_2
            UNION
            SELECT DISTINCT
                * 
            FROM overall_volume_table_3
        ),
        daily_volume_table AS (
            SELECT 
                DATE_TRUNC('day', svt.date) AS day,
                SUM(svt.total_volume) AS daily_volume,
                '{asset_id}' AS asset
                FROM combined_volume_table svt
                WHERE svt.source_id = '{asset_id}' OR svt.dest_id = '{asset_id}'
                GROUP BY DATE_TRUNC('day', svt.date)
        ),
        filled_daily_volume_table AS (
            SELECT 
                ds.day,
                COALESCE(dv.daily_volume, 0) AS daily_volume,
                '{asset_id}' AS asset
            FROM date_series ds
            LEFT JOIN daily_volume_table dv
            ON ds.day = dv.day
        ),
        weekly_averaged_volume_table AS (
            SELECT 
                day,
                asset,
                -- Calculate the 7-day centered moving average
                AVG(daily_volume) OVER (
                    ORDER BY day ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
                ) AS weekly_avg_volume
            FROM filled_daily_volume_table
        )
        SELECT 
            TO_CHAR(day, 'FMMonth FMDD, YYYY') AS day,
            weekly_avg_volume AS total_weekly_avg_volume,
            asset
        FROM weekly_averaged_volume_table
        ORDER BY day
        """
    else:
        query = f"""
        WITH date_series AS (
            -- Generate a series of dates from the minimum to the maximum block timestamp
            SELECT 
                generate_series(
                    (SELECT MIN(DATE_TRUNC('day', block_timestamp)) FROM order_placed), 
                    (SELECT MAX(DATE_TRUNC('day', block_timestamp)) FROM order_placed), 
                    '1 day'::interval
                )::date AS day
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
                op.*, 
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
                svt.order_uuid AS order_id,
                (dvt.dest_volume + svt.source_volume) AS total_volume,
                svt.block_timestamp AS date,
                svt.source_id AS source_id,
                dvt.dest_id AS dest_id
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        ),
        source_volume_table_3 AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        overall_volume_table_3 AS (
            SELECT DISTINCT
                svt.order_uuid AS order_id,
                2*svt.source_volume AS total_volume,
                svt.created_at AS date,
                svt.source_id AS source_id,
                '' AS dest_id
            FROM source_volume_table_3 svt
        ),
        combined_volume_table AS (
            SELECT DISTINCT
                * 
            FROM overall_volume_table_2
            UNION
            SELECT DISTINCT
                * 
            FROM overall_volume_table_3
        ),
        daily_volume_table AS (
            SELECT 
                DATE_TRUNC('day', svt.date) AS day,
                SUM(svt.total_volume) AS daily_volume,
                '{asset_id}' AS asset
                FROM combined_volume_table svt
                GROUP BY DATE_TRUNC('day', svt.date)
        ),
        filled_daily_volume_table AS (
            SELECT 
                ds.day,
                COALESCE(dv.daily_volume, 0) AS daily_volume,
                '{asset_id}' AS asset
            FROM date_series ds
            LEFT JOIN daily_volume_table dv
            ON ds.day = dv.day
        ),
        weekly_averaged_volume_table AS (
            SELECT 
                day,
                asset,
                -- Calculate the 7-day centered moving average
                AVG(daily_volume) OVER (
                    ORDER BY day ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
                ) AS weekly_avg_volume
            FROM filled_daily_volume_table
        )
        SELECT 
            TO_CHAR(day, 'FMMonth FMDD, YYYY') AS day,
            weekly_avg_volume AS total_weekly_avg_volume,
            asset
        FROM weekly_averaged_volume_table
        ORDER BY day
        """
    # Execute the query and return the result as a DataFrame
    return pd.json_normalize(execute_sql(query)['result'])


def get_last_day(asset_id, sd):

    if asset_id != 'Total':

        query = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
            FROM order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        dest_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                dvt.dest_id AS dest_id,
                dvt.dest_chain AS dest_chain,
                dvt.dest_decimal AS dest_decimal,
                dvt.dest_price AS dest_price,
                dvt.dest_volume AS dest_volume,
                (dvt.dest_volume + svt.source_volume) AS total_volume
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.block_timestamp), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM overall_volume_table_2 svt
        WHERE svt.source_id = '{asset_id}' OR svt.dest_id = '{asset_id}'
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp)
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp)
        """

        query_2 = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(created_at)) AS max_date
            FROM mm_order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.created_at >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.created_at < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                2*svt.source_volume AS total_volume
            FROM source_volume_table svt
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.created_at), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM overall_volume_table_2 svt
        WHERE svt.source_id = '{asset_id}'
        GROUP BY DATE_TRUNC('hour', svt.created_at)
        ORDER BY DATE_TRUNC('hour', svt.created_at)
        """
        
    else:
            
        query = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
            FROM order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        dest_volume_table AS (
            SELECT DISTINCT
                op.*, 
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
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                dvt.dest_id AS dest_id,
                dvt.dest_chain AS dest_chain,
                dvt.dest_decimal AS dest_decimal,
                dvt.dest_price AS dest_price,
                dvt.dest_volume AS dest_volume,
                (dvt.dest_volume + svt.source_volume) AS total_volume
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.block_timestamp), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM overall_volume_table_2 svt
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp)
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp)
        """

        query_2 = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(created_at)) AS max_date
            FROM mm_order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.created_at >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.created_at < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                2*svt.source_volume AS total_volume
            FROM source_volume_table svt
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.created_at), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM overall_volume_table_2 svt
        GROUP BY DATE_TRUNC('hour', svt.created_at)
        ORDER BY DATE_TRUNC('hour', svt.created_at)
        """
    #st.write(execute_sql(query))
    return pd.json_normalize(execute_sql(query_2)['result'])

previous_day = today - timedelta(days=1)
# Function to create the datetime object for the prior day
def create_prior_day_datetime(hour_str):
    time_obj = datetime.strptime(hour_str, "%I %p").time()  # Convert "01 PM" to time object
    return datetime.combine(previous_day.date(), time_obj)  # Combine with the prior day's date


asset_list = asset_fetch()
asset_list = asset_list[:15]
asset_list = ['Total'] + asset_list

asset_list_day = asset_fetch_day()
asset_list_day = asset_list_day[:10]
asset_list_day = ['Total'] + asset_list_day

#st.write(asset_list_day)

if "preloaded_2" not in st.session_state:
    preloaded_2 = {}
    for asset in asset_list:
        
        daily_vol = get_volume_vs_date(asset, time_point['oldest_time'][0])
        weekly_vol = get_weekly_volume_vs_date(asset, time_point['oldest_time'][0])
        
        preloaded_2[asset + ' Weekly Average'] = weekly_vol
        preloaded_2[asset + ' Daily Value'] = daily_vol

    for asset in asset_list_day:

        hourly_vol = get_last_day(asset, time_point['oldest_time'][0])
        preloaded_2[asset + ' Hourly Value'] = hourly_vol

        date = today - timedelta(days=7)
        date = date.strftime('%Y-%m-%dT%H:%M:%S')
        
        week_vol = get_volume_vs_date(asset, date)
        preloaded_2[asset + ' Week Volume'] = week_vol

    
    st.session_state["preloaded_2"] = preloaded_2

time_ranges_2 = {
    "All Time": None,  # Special case for no date filter
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}

selected_assets_hourly = st.multiselect("Select Assets", asset_list_day, default=asset_list[:4])

col1, col2 = st.columns(2)
with col1:
    
    st.subheader("Volume By Hour For Latest Calender Day of Active Trading")
    all_assets_data_hour = pd.DataFrame()
    
    # Process individual assets
    for asset in selected_assets_hourly:
            # Fetch data for the selected assets
            data = st.session_state["preloaded_2"][asset + ' Hourly Value']

            # Apply the function to the 'hour' column
            data['date'] = data['hour'].apply(create_prior_day_datetime)
    
            if data.empty:
                st.warning(f"No data available for {asset}!")
            else:
                # Add the 'asset' column (asset name is already included in 'data')
                all_assets_data_hour = pd.concat([all_assets_data_hour, data])
                
    #all_assets_data_hour['hour'] = pd.to_datetime(all_assets_data_hour['hour'])
    # Pivot the data to have separate columns for each asset
    pivot_data = all_assets_data_hour.pivot(index='date', columns='asset', values='total_hourly_volume')
    pivot_data = pivot_data.fillna(0)
    pivot_data = pivot_data.reset_index()
    
    # Melt the data back into long format for Plotly
    melted_data = pivot_data.melt(id_vars=['date'], var_name='asset', value_name='total_hourly_volume')

    # Create an interactive bar chart with Plotly
    fig = px.bar(
        melted_data,
        x='date',
        y='total_hourly_volume',
        color='asset',
        title="Volume By Hour For Latest Calendar Day of Active Trading",
        labels={'date': 'Date & Time', 'total_hourly_volume': 'Volume'},
        hover_data={'date': '|%Y-%m-%d %H:%M:%S', 'total_hourly_volume': True, 'asset': True},
    )

    # Update layout for better readability
    fig.update_layout(
        xaxis_title="Date & Time",
        yaxis_title="Volume",
        legend_title="Asset",
        hovermode="x unified",
    )

    # Render the chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)


with col2:
    st.subheader("Volume In The Last Week")
    all_assets_data_day = pd.DataFrame()
    
    # Process individual assets
            
    data = st.session_state["preloaded_2"]['Total' + ' Week Volume']

    date = today - timedelta(days=7)
    date = date.strftime('%Y-%m-%dT%H:%M:%S')
    
    data = data[pd.to_datetime(data['day']) > pd.to_datetime(date)]

    if data.empty:
        st.warning(f"No data available for {asset}!")
    else:
        # Add the 'asset' column (asset name is already included in 'data')
        all_assets_data_day = pd.concat([all_assets_data_day, data])
    

    all_assets_data_day['day'] = pd.to_datetime(all_assets_data_day['day'])
    # Pivot the data to have separate columns for each asset
    pivot_data = all_assets_data_day.pivot(index='day', columns='asset', values='total_daily_volume')
    pivot_data = pivot_data.fillna(0)
    pivot_data = pivot_data.reset_index()
    # Reset index to make it Plotly-compatible

    # Melt the data back into long format for Plotly
    melted_data = pivot_data.melt(id_vars='day', var_name='asset', value_name='Total Daily Volume')

    # Create an interactive bar chart with Plotly
    fig = px.bar(
        melted_data,
        x='day',
        y='Total Daily Volume',
        color='asset',
        title="Volume In The Last Week",
        labels={'day': 'Date', 'Total Daily Volume': 'Volume'},
        hover_data={'day': '|%Y-%m-%d', 'Total Daily Volume': True, 'asset': True},
    )

    # Update layout for better readability
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Volume",
        legend_title="Asset",
        hovermode="x unified",
    )

    # Render the chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)

##selected_assets_hourly = st.multiselect("Select Assets", asset_list_day, default=asset_list_day[:1])
col1, col2 = st.columns(2)
with col1:
    
    st.subheader("Volume By Hour For Latest Calender Day of Active Trading")
    all_assets_data_hour = pd.DataFrame()
    
    # Process individual assets
    for asset in selected_assets_hourly:
            # Fetch data for the selected assets
            data = st.session_state["preloaded_2"][asset + ' Hourly Value']

            # Apply the function to the 'hour' column
            data['date'] = data['hour'].apply(create_prior_day_datetime)
    
            if data.empty:
                st.warning(f"No data available for {asset}!")
            else:
                # Add the 'asset' column (asset name is already included in 'data')
                all_assets_data_hour = pd.concat([all_assets_data_hour, data])
                
    
    #all_assets_data_hour['hour'] = pd.to_datetime(all_assets_data_hour['hour'])
    # Pivot the data to have separate columns for each asset
    pivot_data = all_assets_data_hour.pivot(index='date', columns='asset', values='total_hourly_volume')
    pivot_data = pivot_data.fillna(0)

    # Reset index and melt for Altair
    melted_data = pivot_data.reset_index().melt(id_vars='date', var_name='Asset', value_name='Total Hourly Volume')
    
    # Base line chart
    base = alt.Chart(melted_data).mark_line().encode(
        x=alt.X('date:T', title="Date and Time"),  # Datetime on x-axis
        y=alt.Y('Total Hourly Volume:Q', title="Total Hourly Volume"),  # Volume on y-axis
        color=alt.Color('Asset:N', legend=None),  # Remove legend
    )
    
    # Add an interactive rule to hover over the graph
    hover = alt.selection_single(
        fields=["date"],
        nearest=True,
        on="mouseover",
        empty="none",
        clear="mouseout"
    )
    
    # Highlight points at the hovered timestamp
    points = base.mark_circle(size=65).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    ).add_selection(hover)
    
    # Custom tooltip with formatted date and time
    chart = alt.layer(
        base,
        points
    ).encode(
        tooltip=[
            alt.Tooltip('date:T', title='Date & Time', format='%B, %d %Y at %I:%M %p'),  # Custom format
            alt.Tooltip('Asset:N', title='Asset'),
            alt.Tooltip('Total Hourly Volume:Q', title='Volume'),
        ]
    ).interactive()
    
    # Display in Streamlit
    st.altair_chart(chart, use_container_width=True)

with col2:
    st.subheader("Volume In The Last Week")
    all_assets_data_day = pd.DataFrame()
    
    # Process individual assets
    for asset in selected_assets_hourly:
            # Fetch data for the selected assets
            
            data = st.session_state["preloaded_2"][asset + ' Week Volume']
    
            date = today - timedelta(days=7)
            date = date.strftime('%Y-%m-%dT%H:%M:%S')
            
            data = data[pd.to_datetime(data['day']) > pd.to_datetime(date)]
    
            if data.empty:
                st.warning(f"No data available for {asset}!")
            else:
                # Add the 'asset' column (asset name is already included in 'data')
                all_assets_data_day = pd.concat([all_assets_data_day, data])
    

    all_assets_data_day['day'] = pd.to_datetime(all_assets_data_day['day'])
    # Pivot the data to have separate columns for each asset
    pivot_data = all_assets_data_day.pivot(index='day', columns='asset', values='total_daily_volume')
    pivot_data = pivot_data.fillna(0)
    
    # Create a full range of hours for the day
    #full_hour_range = pd.date_range(start=pivot_data.index.min(), end=pivot_data.index.max(), freq='H')
    #pivot_data = pivot_data.reindex(full_hour_range)
    
    # Fill gaps using interpolation
    #pivot_data = pivot_data.interpolate(method='linear')  # Use linear interpolation for smooth filling
    
    # Plot the cumulative data using st.line_chart
    st.line_chart(pivot_data, use_container_width=True)


col1, col2 = st.columns(2)

time_ranges_2 = {
    "All Time": None,  # Special case for no date filter
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}

selected_range_2 = st.selectbox("Select a time range for the long-term volume display:", list(time_ranges_2.keys()))

# Calculate the start date
if time_ranges[selected_range_2] is not None:
    start_date_2 = today - timedelta(days=time_ranges[selected_range_2])
    start_date_2 = start_date_2.strftime('%Y-%m-%dT%H:%M:%S')
    #st.write(start_date)
else:
    start_date_2 = time_point['oldest_time'][0]  # No filter for "All Time"
    #st.write(start_date)
# Multi-select assets
selected_assets = st.multiselect("Select Assets", asset_list, default=asset_list[:4])

# Initialize an empty DataFrame to collect data for all assets
all_assets_data = pd.DataFrame()


#st.write(pd.to_datetime(start_date_2))
#st.write(pd.to_datetime(st.session_state["preloaded_2"]['Total Daily Value']['day']))

col1, col2 = st.columns(2)
with col2:
    st.subheader("Total Volume")
    # Initialize an empty DataFrame to collect data for all assets, including "Total"
    all_assets_data = pd.DataFrame()

    # Process individual assets
    for asset in selected_assets:
            # Fetch data for the selected assets
            data = st.session_state["preloaded_2"][asset + ' Daily Value']
            data = data[pd.to_datetime(data['day']) > pd.to_datetime(start_date_2)]

            if data.empty:
                st.warning(f"No data available for {asset}!")
            else:
                # Add the 'asset' column (asset name is already included in 'data')
                all_assets_data = pd.concat([all_assets_data, data])

    
    # Ensure the 'day' column is of datetime type
    all_assets_data['day'] = pd.to_datetime(all_assets_data['day'])

    # Pivot the data to have separate columns for each asset
    pivot_data = all_assets_data.pivot(index='day', columns='asset', values='total_daily_volume')

    # Reindex to fill in missing dates
    full_date_range = pd.date_range(start=pivot_data.index.min(), end=pivot_data.index.max())
    pivot_data = pivot_data.reindex(full_date_range)

    # Fill gaps using interpolation
    pivot_data = pivot_data.interpolate(method='linear')  # Use linear interpolation for smooth filling

    # Calculate cumulative sum for each asset
    cumulative_data = pivot_data.cumsum()

    # Plot the cumulative data using st.line_chart
    st.line_chart(cumulative_data, use_container_width=True)

    
#with col2:
    
#    st.subheader("Volume")
    # Initialize an empty DataFrame to collect data for all assets, including "Total"
#    all_assets_data = pd.DataFrame()

    # Check if "Total" is in the selected assets
#    if "Total" in selected_assets:
#        total_data = dfs["daily_volume"].copy()

        # Add an 'asset' column to distinguish the "Total" data
#        total_data["asset"] = "Total"

        # Append to the all_assets_data DataFrame
#       all_assets_data = pd.concat([all_assets_data, total_data])

    # Process individual assets
#    for asset in selected_assets:
#        if asset != "Total":
            # Fetch data for the selected assets
#            data = get_volume_vs_date(asset)

#            if data.empty:
#                st.warning(f"No data available for {asset}!")
#            else:
                # Add the 'asset' column (asset name is already included in 'data')
#                all_assets_data = pd.concat([all_assets_data, data])

    # Ensure the 'day' column is of datetime type
#    all_assets_data['day'] = pd.to_datetime(all_assets_data['day'])

    # Pivot the data to have separate columns for each asset
#    pivot_data = all_assets_data.pivot(index='day', columns='asset', values='total_daily_volume')

    # Ensure every selected asset has a column in pivot_data
#    for asset in selected_assets:
#        if asset not in pivot_data.columns:
            # If the column doesn't exist for the asset, create it with NaN values
#            pivot_data[asset] = pd.NA
    # Plot the combined data using st.line_chart
#    st.line_chart(pivot_data, use_container_width=True)

with col1:
    st.subheader("Weekly Average Volume")
    # Initialize an empty DataFrame to collect data for all assets, including "Total"
    all_assets_data = pd.DataFrame()

    # Process individual assets
    for asset in selected_assets:
        if asset != "Total":
            # Fetch data for the selected assets
            data = st.session_state["preloaded_2"][asset + ' Weekly Average']
            data = data[pd.to_datetime(data['day']) > pd.to_datetime(start_date_2)]

            if data.empty:
                st.warning(f"No data available for {asset}!")
            else:
                # Add the 'asset' column (asset name is already included in 'data')
                all_assets_data = pd.concat([all_assets_data, data])

        else:

            data = st.session_state["preloaded_2"]['Total Weekly Average']
            data = data[pd.to_datetime(data['day']) > pd.to_datetime(start_date_2)]

            if data.empty:
                st.warning(f"No data available for Total!")
            else:
                # Add the 'asset' column (asset name is already included in 'data')
                all_assets_data = pd.concat([all_assets_data, data])
            
    # Ensure the 'day' column is of datetime type
    all_assets_data['day'] = pd.to_datetime(all_assets_data['day'])

    # Pivot the data to have separate columns for each asset
    pivot_data = all_assets_data.pivot(index='day', columns='asset', values='total_weekly_avg_volume')

    # Ensure every selected asset has a column in pivot_data
    for asset in selected_assets:
        if asset not in pivot_data.columns:
            # If the column doesn't exist for the asset, create it with NaN values
            pivot_data[asset] = pd.NA
    # Plot the combined data using st.line_chart
    st.line_chart(pivot_data, use_container_width=True)

col1, col2 = st.columns(2)

time_ranges_3 = {
    "All Time": None,  # Special case for no date filter
    "Last Week": 7,
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}

# Get today's date
#today = datetime.now()


# Calculate the start date
#if time_ranges[selected_range_3] is not None:
    #start_date_3 = today - timedelta(days=time_ranges_3[selected_range_3])
    #start_date_3 = start_date_3.strftime('%Y-%m-%dT%H:%M:%S')
    #st.write(start_date)
#else:
    #start_date_3 = time_point['oldest_time'][0]  # No filter for "All Time"
    #st.write(start_date)


# Initialize session state if not already present
#if 'df_trade_address' not in st.session_state:
    #st.session_state.df_trade_address = df_trade_address
    #st.session_state.df_volume_address = df_volume_address
    #st.session_state.page_trade = 0
    #st.session_state.page_volume = 0

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
    if direction == 'next':
        st.session_state[page_key] += 1
    elif direction == 'previous':
        st.session_state[page_key] -= 1
    st.rerun()

@st.cache_data
def histogram_data(sd):
    # Supabase credentials
    supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
    supabase_key = st.secrets["supabase_key"]
    sql_query1 = f"""
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
WHERE op.block_timestamp >= '{sd}'
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
WHERE op.block_timestamp >= '{sd}'
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
        source_chain, 
        source_id, 
        SUM(source_volume) AS source_volume
    FROM 
        overall_volume_table_2
    GROUP BY 
        source_chain, 
        source_id
    ORDER BY 
        source_chain, 
        source_id
    """

    sql_query2 = f"""
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
WHERE op.block_timestamp >= '{sd}'
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
WHERE op.block_timestamp >= '{sd}'
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
        dest_chain, 
        dest_id, 
        SUM(dest_volume) AS dest_volume
    FROM 
        overall_volume_table_2
    GROUP BY 
        dest_chain, 
        dest_id
    ORDER BY 
        dest_chain, 
        dest_id
    """

    sql_query3 = f"""
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
WHERE op.block_timestamp >= '{sd}'
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
WHERE op.block_timestamp >= '{sd}'
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
        chain AS chain,
        asset AS asset,
        SUM(volume) AS total_volume
    FROM (
        -- Treat source_chain/source_id as one group
        SELECT 
            source_chain AS chain, 
            source_id AS asset, 
            source_volume AS volume
        FROM 
            overall_volume_table_2

        UNION ALL

        -- Treat dest_chain/dest_id as another group
        SELECT 
            dest_chain AS chain, 
            dest_id AS asset, 
            dest_volume AS volume
        FROM 
            overall_volume_table_2
    ) AS combined_data
    GROUP BY 
        chain, 
        asset
    ORDER BY 
        chain, 
        asset
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
    df_source_chain_volume = execute_sql(sql_query1)
    df_dest_chain_volume = execute_sql(sql_query2)
    df_total_chain_volume = execute_sql(sql_query3)


    df_source_chain_volume = pd.json_normalize(df_source_chain_volume['result'])
    df_dest_chain_volume = pd.json_normalize(df_dest_chain_volume['result'])        
    df_total_chain_volume = pd.json_normalize(df_total_chain_volume['result'])


    # User filter for source pairs    
    #st.sidebar.header("Source Volume Filters")
    #source_chains = st.sidebar.multiselect(
    #    "Select Source Chains", 
    #    options=df_source_chain_volume["source_chain"].unique(),
    #    default=df_source_chain_volume["source_chain"].unique()
    #)

    source_chains = df_source_chain_volume["source_chain"].unique()
    #source_ids = st.sidebar.multiselect(
    #    "Select Source IDs", 
    #    options=df_source_chain_volume["source_id"].unique(),
    #    default=df_source_chain_volume["source_id"].unique()
    #)
    source_ids = df_source_chain_volume["source_id"].unique()
    # User filter for destination pairs
    
    #st.sidebar.header("Destination Volume Filters")
    # dest_chains = st.sidebar.multiselect(
    #    "Select Destination Chains", 
    #    options=df_dest_chain_volume["dest_chain"].unique(),
    #    default=df_dest_chain_volume["dest_chain"].unique()
    #)
    dest_chains = df_dest_chain_volume["dest_chain"].unique()
    
    #dest_ids = st.sidebar.multiselect(
    #    "Select Destination IDs", 
    #    options=df_dest_chain_volume["dest_id"].unique(),
    #    default=df_dest_chain_volume["dest_id"].unique()
    #)
    dest_ids = df_dest_chain_volume["dest_id"].unique()
    # User filter for total volume pairs
    #st.sidebar.header("Total Volume Filters")
    #total_chains = st.sidebar.multiselect(
    #    "Select Chains", 
    #    options=df_total_chain_volume["chain"].unique(),
    #    default=df_total_chain_volume["chain"].unique()
    #)
    total_chains = df_total_chain_volume["chain"].unique()
    #total_assets = st.sidebar.multiselect(
    #    "Select Assets", 
    #    options=df_total_chain_volume["asset"].unique(),
    #    default=df_total_chain_volume["asset"].unique()
    #)
    total_assets = df_total_chain_volume["asset"].unique()

    return {
            "df_source_chain_volume": df_source_chain_volume,
            "source_chains": source_chains,
            "source_ids": source_ids,
            "df_dest_chain_volume": df_dest_chain_volume,
            "dest_chains": dest_chains,
            "dest_ids": dest_ids,
            "df_total_chain_volume": df_total_chain_volume,
            "total_chains": total_chains,
            "total_assets": total_assets,
        }

def vol_hist_and_pie(load):
    df_source_chain_volume = load['df_source_chain_volume']
    source_chains = load['source_chains']
    source_ids = load['source_ids']
    df_dest_chain_volume = load['df_dest_chain_volume']
    dest_chains = load['dest_chains']
    dest_ids = load['dest_ids']
    df_total_chain_volume = load['df_total_chain_volume']
    total_chains = load['total_chains']
    total_assets = load['total_assets']
    
    with st.container():
        col1, col2, col3 = st.columns([1, 1, 1])

        with col1:
            st.subheader("Source Volume")
            st.write("Volume on Source Chains for the 16 Largest Pairs")
            # Apply filters
            filtered_source_df = df_source_chain_volume[
                (df_source_chain_volume["source_chain"].isin(source_chains)) & 
                (df_source_chain_volume["source_id"].isin(source_ids))
            ]

            grouped_df = filtered_source_df.groupby(["source_chain", "source_id"], as_index=False)["source_volume"].sum()
            grouped_df = grouped_df.nlargest(16, "source_volume")
            chain_order = grouped_df.groupby("source_chain")["source_volume"].sum().sort_values(ascending=False).index

            base = alt.Chart(grouped_df).mark_bar().encode(
                x=alt.X("source_chain:N", title="Source Chain", sort=chain_order, axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("source_volume:Q", title="Total Volume"),
                color=alt.Color("source_id:N", title="Source ID", scale=alt.Scale(scheme="category20")),
                tooltip=["source_chain", "source_id", "source_volume"]
            )

            highlight = alt.selection_single(on="mouseover", fields=["source_chain", "source_id"], nearest=True, empty="none")
            highlighted_chart = base.encode(opacity=alt.condition(highlight, alt.value(1), alt.value(0.6))).add_selection(highlight)

            st.altair_chart(highlighted_chart, use_container_width=True)

        with col2:
            st.subheader("Destination Volume")
            st.write("Volume on Destination Chains for the 16 Largest Pairs")
            # Apply filters
            filtered_dest_df = df_dest_chain_volume[
                (df_dest_chain_volume["dest_chain"].isin(dest_chains)) & 
                (df_dest_chain_volume["dest_id"].isin(dest_ids))
            ]

            grouped_df = filtered_dest_df.groupby(["dest_chain", "dest_id"], as_index=False)["dest_volume"].sum()
            grouped_df = grouped_df.nlargest(16, "dest_volume")
            chain_order = grouped_df.groupby("dest_chain")["dest_volume"].sum().sort_values(ascending=False).index

            base = alt.Chart(grouped_df).mark_bar().encode(
                x=alt.X("dest_chain:N", title="Destination Chain", sort=chain_order, axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("dest_volume:Q", title="Total Volume"),
                color=alt.Color("dest_id:N", title="Destination ID", scale=alt.Scale(scheme="category20")),
                tooltip=["dest_chain", "dest_id", "dest_volume"]
            )

            highlight = alt.selection_single(on="mouseover", fields=["dest_chain", "dest_id"], nearest=True, empty="none")
            highlighted_chart = base.encode(opacity=alt.condition(highlight, alt.value(1), alt.value(0.6))).add_selection(highlight)

            st.altair_chart(highlighted_chart, use_container_width=True)

        with col3:
            st.subheader("Total Volume")
            st.write("Total Volume on Chains for the 16 Largest Pairs")
            # Apply filters
            filtered_total_df = df_total_chain_volume[
                (df_total_chain_volume["chain"].isin(total_chains)) & 
                (df_total_chain_volume["asset"].isin(total_assets))
            ]

            grouped_df = filtered_total_df.groupby(["chain", "asset"], as_index=False)["total_volume"].sum()
            grouped_df = grouped_df.nlargest(16, "total_volume")
            chain_order = grouped_df.groupby("chain")["total_volume"].sum().sort_values(ascending=False).index

            base = alt.Chart(grouped_df).mark_bar().encode(
                x=alt.X("chain:N", title="Chain", sort=chain_order, axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("total_volume:Q", title="Total Volume"),
                color=alt.Color("asset:N", title="Asset ID", scale=alt.Scale(scheme="category20")),
                tooltip=["chain", "asset", "total_volume"]
            )

            highlight = alt.selection_single(on="mouseover", fields=["chain", "asset"], nearest=True, empty="none")
            highlighted_chart = base.encode(opacity=alt.condition(highlight, alt.value(1), alt.value(0.6))).add_selection(highlight)

            st.altair_chart(highlighted_chart, use_container_width=True)
        # Calculate total volume by asset
        asset_volume = df_total_chain_volume.groupby('asset')['total_volume'].sum().reset_index()
        asset_volume['percent'] = 100 * asset_volume['total_volume'] / asset_volume['total_volume'].sum()

    # Create the first pie chart for asset distribution using Altair
    pie_asset = alt.Chart(asset_volume).mark_arc().encode(
        theta=alt.Theta(field="total_volume", type="quantitative"),
        color=alt.Color(field="asset", type="nominal"),
        tooltip=['asset', 'total_volume', 'percent']
    ).properties(
        title="Distribution of Total Volume on Each Asset For Top 10 Assets"
    )

    # Streamlit layout for filtering

    # Assuming the filters for chains and pairs are defined earlier in the app
    # Example: 'selected_chains' and 'selected_assets' from multiselect widgets or filtering logic

    # Filter the dataframe based on the user's selection
    filtered_total_df = df_total_chain_volume[
        (df_total_chain_volume["chain"].isin(total_chains)) & 
        (df_total_chain_volume["asset"].isin(total_assets))
    ]

    # Recalculate total volume by chain for the filtered data
    chain_volume = filtered_total_df.groupby('chain')['total_volume'].sum().reset_index()
    chain_volume['percent'] = 100 * chain_volume['total_volume'] / chain_volume['total_volume'].sum()

    chain_volume = chain_volume.nlargest(10, 'percent')
    # Create the pie chart for chain distribution using Altair
    pie_chain = alt.Chart(chain_volume).mark_arc().encode(
        theta=alt.Theta(field="total_volume", type="quantitative"),
        color=alt.Color(field="chain", type="nominal", title="Chain"),
        tooltip=['chain', 'total_volume', 'percent']
    ).properties(
        title="Distribution of Total Volume On Each Chain For Top 10 Chains"
    )

    # Recalculate total volume by asset for the filtered data
    asset_volume = filtered_total_df.groupby('asset')['total_volume'].sum().reset_index()
    asset_volume['percent'] = 100 * asset_volume['total_volume'] / asset_volume['total_volume'].sum()

    asset_volume = asset_volume.nlargest(10, 'percent')
    # Create the pie chart for asset distribution using Altair
    pie_asset = alt.Chart(asset_volume).mark_arc().encode(
        theta=alt.Theta(field="total_volume", type="quantitative"),
        color=alt.Color(field="asset", type="nominal", title="Asset"),
        tooltip=['asset', 'total_volume', 'percent']
    ).properties(
        title="Distribution of Total Volume For Each Asset For Top 10 Assets"
    )

    # Display the pie charts
    st.subheader("Volume by Asset")
    st.altair_chart(pie_asset, use_container_width=True)
    
    st.subheader("Volume by Chain")
    st.altair_chart(pie_chain, use_container_width=True)

if "preloaded_3" not in st.session_state:
    preloaded_3 = {}
    for i in day_list:
        date = today - timedelta(days=i)
        date = date.strftime('%Y-%m-%dT%H:%M:%S')
    
        data = histogram_data(date)
        preloaded_3[i] = data
    
    date = time_point['oldest_time'][0]
    data = histogram_data(date)
    preloaded_3[0] = data

    st.session_state["preloaded_3"] = preloaded_3

selected_range_3 = st.selectbox("Select a time range for the volume distribution data:", list(time_ranges.keys()))

if time_ranges[selected_range_3] is not None:
    vol_hist_and_pie(st.session_state["preloaded_3"][time_ranges[selected_range_3]])
else:
    vol_hist_and_pie(st.session_state["preloaded_3"][0])

st.title("User Analysis")
time_ranges_4 = {
    "All Time": None,  # Special case for no date filter
    "Last Week": 7,
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}

# Get today's date
#today = datetime.now()

selected_range_4 = st.selectbox("Select a time range for the user distribution histograms:", list(time_ranges_4.keys()))

# Calculate the start date
if time_ranges[selected_range_4] is not None:
    start_date_4 = today - timedelta(days=time_ranges_4[selected_range_4])
    start_date_4 = start_date_4.strftime('%Y-%m-%dT%H:%M:%S')
    #st.write(start_date)
else:
    start_date_4 = time_point['oldest_time'][0]  # No filter for "All Time"
    #st.write(start_date)

def user_analysis_data(sd):
    sql_query12 = f""" 
    WITH RankedTrades AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY COUNT(order_id) DESC) AS rank,
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
        WHERE op.block_timestamp >= '{sd}'
    ) AS all_trades
    GROUP BY address
    ),
    CumulativeTrades AS (
    SELECT
        rank AS N,
        SUM(trade_count) OVER (ORDER BY rank) AS cumulative_trade_count,
        (SELECT SUM(trade_count) FROM RankedTrades) AS total_trades
    FROM RankedTrades
    WHERE rank <= 200
    )
    SELECT
        N,
        CAST(cumulative_trade_count * 100.0 / total_trades AS FLOAT) AS percentage_of_total_trades
    FROM CumulativeTrades
    ORDER BY N
    """

    sql_query13 = f"""
    WITH source_volume_table AS (
    SELECT DISTINCT
        op.order_uuid, 
        op.source_quantity, 
        op.source_asset,
        op.sender_address,  
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
    WHERE op.block_timestamp >= '{sd}'
    ),
    dest_volume_table AS (
        SELECT DISTINCT
            op.order_uuid, 
            op.dest_quantity, 
            op.dest_asset,
            me.maker_address,  
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
        WHERE op.block_timestamp >= '{sd}'
    ),
    overall_volume_table_2 AS (
        SELECT DISTINCT
            svt.order_uuid,
            svt.sender_address,  
            dvt.maker_address,   
            svt.source_volume,
            dvt.dest_volume,
            (dvt.dest_volume + svt.source_volume) AS total_volume
        FROM source_volume_table svt
        INNER JOIN dest_volume_table dvt
            ON svt.order_uuid = dvt.order_uuid
    ),
    total_volume_table AS (
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
    ),
    ranked_volume_table AS (
        SELECT 
            address,
            total_user_volume,
            RANK() OVER (ORDER BY total_user_volume DESC) AS rank
        FROM total_volume_table
    ),
    cumulative_volume_table AS (
        SELECT 
            rank,
            SUM(total_user_volume) OVER (ORDER BY rank) AS cumulative_volume,
            (SUM(total_user_volume) OVER (ORDER BY rank) * 100.0) / (SUM(total_user_volume) OVER ()) AS percentage_of_total_volume
        FROM ranked_volume_table
    )
    SELECT 
        rank AS top_n,
        percentage_of_total_volume
    FROM cumulative_volume_table
    WHERE rank <= 300
    """
    
    
    trade_add_query = f"""
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
            WHERE op.block_timestamp >= '{sd}'
        ) AS all_trades
        GROUP BY address
        ORDER BY trade_count DESC
        LIMIT 200
        """
        
    volume_add_query = f"""
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
        WHERE op.block_timestamp >= '{sd}'
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
        WHERE op.block_timestamp >= '{sd}'
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
        LIMIT 200
        """
    
    df_trade_rank_1 = execute_sql(sql_query12)
    df_volume_rank_1 = execute_sql(sql_query13)
    
    df_trade_rank_1 = pd.json_normalize(df_trade_rank_1['result'])
    df_volume_rank_1 = pd.json_normalize(df_volume_rank_1['result'])
    
    # Limit to the first 30 rows
    df_trade_rank_1 = df_trade_rank_1.head(10)
    df_volume_rank_1 = df_volume_rank_1.head(10)

    df_trade_rank_1['percentage_of_total_trades'] = df_trade_rank_1['percentage_of_total_trades'].round(1)
    df_volume_rank_1['percentage_of_total_volume'] = df_volume_rank_1['percentage_of_total_volume'].round(1)

    df_trade_address_2 = execute_sql(trade_add_query)
    df_volume_address_2 = execute_sql(volume_add_query)
    
    df_trade_address_2 = pd.json_normalize(df_trade_address_2['result'])
    df_volume_address_2 = pd.json_normalize(df_volume_address_2['result'])

    df_trade_address_2.index = df_trade_address_2.index+1
    df_volume_address_2.index = df_volume_address_2.index+1
    return {
                "df_trade_rank_1": df_trade_rank_1,
                "df_volume_rank_1": df_volume_rank_1,
                "df_trade_address_2": df_trade_address_2,
                "df_volume_address_2": df_volume_address_2,
           }

# Add percentage columns to session state dataframes
#df_trade_address['percentage_of_total_trades'] = (
#    df_trade_address['trade_count'] / (2*total_trades) * 100
#).round(2)

#df_volume_address['percentage_of_total_volume'] = (
#    df_volume_address['total_user_volume'] / (2*total_volume) * 100
#).round(2)


def user_analysis_displays(load):    
    
    df_trade_rank = load['df_trade_rank_1']
    df_volume_rank = load['df_volume_rank_1']
    # Create the bar chart
    fig = px.bar(
        df_trade_rank,
        x='n',  # Top N users
        y='percentage_of_total_trades',  # Percentage
        text='percentage_of_total_trades',  # Show percentage values on the bars
        labels={'n': 'Top N Users', 'percentage_of_total_trades': 'Percentage of Total Trades'},
        title='Percentage of Total Trades Comprised of Up To the Top 10 Users In Terms of Most Trades',
    )
    
    # Customize the appearance
    fig.update_traces(marker_color='blue', textposition='outside')
    fig.update_layout(
        template='plotly_white',
        height=500,
        width=800
    )
    
    st.subheader("User Portion by Trade")
    # Show chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)
    
    # Create the bar chart
    fig = px.bar(
        df_volume_rank,
        x='top_n',  # Top N users
        y='percentage_of_total_volume',  # Percentage
        text='percentage_of_total_volume',  # Show percentage values on the bars
        labels={'top_n': 'Top N Users', 'percentage_of_total_volume': 'Percentage of Total Trades'},
        title='Percentage of Total Volume Comprised of Up To the Top 10 Users In Terms of Most Trades',
    )
    
    # Customize the appearance
    fig.update_traces(marker_color='blue', textposition='outside')
    fig.update_layout(
        template='plotly_white',
        height=500,
        width=800
    )
    st.subheader("User Portion by Volume")
    # Show chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)

    df_trade_address = load['df_trade_address_2']
    df_volume_address = load['df_volume_address_2']
    # For 'Users With The Most Trades' section
    col1, col2 = st.columns([2, 2])  # Adjusting width to match your content layout
    with col1:
        st.subheader("Users With The Most Trades")
    
        # Rename columns for clarity
        renamed_df_trade = df_trade_address.rename(columns={
            'address': 'User ID',
            'trade_count': 'Number of Trades',
            'percentage_of_total_trades': '% of Total Trades',
            'some_other_column': 'Other Info'
        })
    
        # Get the paginated DataFrame
        #df_paginated_trade = paginate_df(renamed_df_trade, st.session_state.page_trade)
    
        # Add index starting at 1 for display
        #df_paginated_trade.index = df_paginated_trade.index + 1
        # Display the DataFrame
        st.write(renamed_df_trade)
    
        # Calculate total pages for trade data
        #total_pages_trade = calculate_total_pages(st.session_state.df_trade_address)
    
        # Use st.empty() to ensure buttons are properly placed
        #button_col_left = st.empty()
        #button_col_right = st.empty()
    
        # Add buttons for pagination
        #if st.session_state.page_trade > 0:
        #    if st.button('Previous', key=f'prev_trade_{st.session_state.page_trade}'):
        #        handle_page_change('page_trade', 'previous', total_pages_trade)
    
        #if st.session_state.page_trade < total_pages_trade - 1:
        #    if st.button('Next', key=f'next_trade_{st.session_state.page_trade}'):
        #        handle_page_change('page_trade', 'next', total_pages_trade)
    
    # For 'Users With The Most Volume' section
    with col2:
        st.subheader("Users With The Most Volume")
    
        # Rename columns for clarity
        renamed_df_volume = df_volume_address.rename(columns={
            'address': 'User ID',
            'total_user_volume': 'Volume',
            'percentage_of_total_volume': '% of Total Volume',
            'another_column': 'Some Other Data'
        })
    
        # Get the paginated DataFrame
        #df_paginated_volume = paginate_df(renamed_df_volume, st.session_state.page_volume)
    
        # Add index starting at 1 for display
        #df_paginated_volume.index = df_paginated_volume.index + 1
        # Display the DataFrame
        st.write(renamed_df_volume)

if "preloaded_4" not in st.session_state:
    preloaded_4 = {}
    for i in day_list:
        date = today - timedelta(days=i)
        date = date.strftime('%Y-%m-%dT%H:%M:%S')
    
        data = user_analysis_data(date)
        preloaded_4[i] = data
    
    date = time_point['oldest_time'][0]
    data = user_analysis_data(date)
    preloaded_4[0] = data

    st.session_state["preloaded_4"] = preloaded_4

if time_ranges[selected_range_4] is not None:
    user_analysis_displays(st.session_state["preloaded_4"][time_ranges[selected_range_4]])
else:
    user_analysis_displays(st.session_state["preloaded_4"][0])

# Supabase credentials
supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
supabase_key = st.secrets["supabase_key"]

# SQL query
@st.cache_data
def execute_sql(query):
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json"
    }
    rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
    payload = {"query": query}
    response = requests.post(rpc_endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        print("Error executing query:", response.status_code, response.json())
        return pd.DataFrame()

# Generate and display charts
st.title("Flow Charts")

time_ranges_5 = {
    "All Time": None,  # Special case for no date filter
    "Last Week": 7,
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}

selected_range_5 = st.selectbox("Select a time range for the flow charts:", list(time_ranges_5.keys()))

# Calculate the start date
if time_ranges[selected_range_5] is not None:
    start_date_5 = today - timedelta(days=time_ranges_5[selected_range_5])
    start_date_5 = start_date_5.strftime('%Y-%m-%dT%H:%M:%S')
    #st.write(start_date)
else:
    start_date_5 = time_point['oldest_time'][0]  # No filter for "All Time"
    #st.write(start_date)


@st.cache_data
def sankey_data(sd):
    sql_query = f"""
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
        ON op.source_asset = ti.address
      INNER JOIN coingecko_assets_list cal
        ON op.source_asset = cal.address
      INNER JOIN coingecko_market_data cmd 
        ON cal.id = cmd.id
      WHERE op.block_timestamp >= '{sd}'
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
        ON op.dest_asset = ti.address
      INNER JOIN coingecko_assets_list cal
        ON op.dest_asset = cal.address
      INNER JOIN coingecko_market_data cmd 
        ON cal.id = cmd.id
      WHERE op.block_timestamp >= '{sd}'
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
        source_chain,
        source_id,
        dest_chain,
        dest_id,
        SUM(source_volume) AS total_source_volume,
        SUM(dest_volume) AS total_dest_volume
    FROM 
        overall_volume_table_2
    GROUP BY 
        source_chain, source_id, dest_chain, dest_id
    ORDER BY 
        total_source_volume DESC
    """
    
    # Fetch and normalize data
    data = execute_sql(sql_query)
    data = pd.json_normalize(data['result'])
    
    # Label sources and destinations for assets and chains
    data["source_chain"] = data["source_chain"] + " (S)"
    data["dest_chain"] = data["dest_chain"] + " (D)"
    data["source_id"] = data["source_id"] + " (S)"
    data["dest_id"] = data["dest_id"] + " (D)"

    # Combine asset and chain for source and destination pairs, appending (S) and (D)
    data["source_pair"] = data["source_id"] + " | " + data["source_chain"] + " (S)"
    data["dest_pair"] = data["dest_id"] + " | " + data["dest_chain"] + " (D)"
    
    # Compute total volume for assets
    source_asset_volume = data.groupby("source_id")["total_source_volume"].sum().reset_index()
    dest_asset_volume = data.groupby("dest_id")["total_dest_volume"].sum().reset_index()

    # Group and sum volumes for asset-chain pairs
    pair_volume = data.groupby(["source_pair", "dest_pair"], as_index=False).agg({
        "total_source_volume": "sum",
        "total_dest_volume": "sum"
    })

    # Compute average volume for pairs
    pair_volume["avg_volume"] = (
        pair_volume["total_source_volume"] + pair_volume["total_dest_volume"]
    ) / 2

    # Select the top 10 asset-chain pairs by avg_volume
    top_pair_data = pair_volume.nlargest(10, "avg_volume")

    # Combine source and destination volumes for assets
    asset_volume = pd.concat([
        source_asset_volume.rename(columns={"source_id": "asset", "total_source_volume": "total_volume"}),
        dest_asset_volume.rename(columns={"dest_id": "asset", "total_dest_volume": "total_volume"})
    ])
    asset_volume = asset_volume.groupby("asset")["total_volume"].sum().reset_index()
    
    # Get top 10 assets by total volume
    top_assets = asset_volume.nlargest(10, "total_volume")["asset"]
    
    # Create a new dataframe for the top 10 assets
    top_asset_data = data[
        data["source_id"].isin(top_assets) | data["dest_id"].isin(top_assets)
    ][["source_id", "dest_id", "total_source_volume", "total_dest_volume"]].copy()
    
    # Compute total volume for chains
    source_chain_volume = data.groupby("source_chain")["total_source_volume"].sum().reset_index()
    dest_chain_volume = data.groupby("dest_chain")["total_dest_volume"].sum().reset_index()
    
    # Combine source and destination volumes for chains
    chain_volume = pd.concat([
        source_chain_volume.rename(columns={"source_chain": "chain", "total_source_volume": "total_volume"}),
        dest_chain_volume.rename(columns={"dest_chain": "chain", "total_dest_volume": "total_volume"})
    ])
    chain_volume = chain_volume.groupby("chain")["total_volume"].sum().reset_index()
    
    # Get top 10 chains by total volume
    top_chains = chain_volume.nlargest(10, "total_volume")["chain"]
    
    # Create a new dataframe for the top 10 chains
    top_chain_data = data[
        data["source_chain"].isin(top_chains) | data["dest_chain"].isin(top_chains)
    ][["source_chain", "dest_chain", "total_source_volume", "total_dest_volume"]].copy()
    
    # Consolidate data for assets by grouping and summing volumes
    consolidated_asset_data = top_asset_data.groupby(
        ["source_id", "dest_id"], as_index=False
    ).agg({
        "total_source_volume": "sum",
        "total_dest_volume": "sum"
    })
    
    # Compute average volume for consolidated asset data
    consolidated_asset_data["avg_volume"] = (
        consolidated_asset_data["total_source_volume"] + consolidated_asset_data["total_dest_volume"]
    ) / 2
    
    # Select top 10 rows based on avg_volume
    top_asset_data = consolidated_asset_data.nlargest(10, "avg_volume")
    
    # Consolidate data for chains by grouping and summing volumes
    consolidated_chain_data = top_chain_data.groupby(
        ["source_chain", "dest_chain"], as_index=False
    ).agg({
        "total_source_volume": "sum",
        "total_dest_volume": "sum"
    })
    
    # Compute average volume for consolidated chain data
    consolidated_chain_data["avg_volume"] = (
        consolidated_chain_data["total_source_volume"] + consolidated_chain_data["total_dest_volume"]
    ) / 2
    
    # Select top 10 rows based on avg_volume
    top_chain_data = consolidated_chain_data.nlargest(10, "avg_volume")
    # Adjust Sankey function to handle filtered dataframes
    return {
            "top_asset_data": top_asset_data,
            "top_chain_data": top_chain_data,
            "top_pair_data": top_pair_data,  # New data for asset-chain pairs
    }
    
def create_sankey_chart(df, source_col, target_col, value_col):
    unique_nodes = list(pd.unique(df[[source_col, target_col]].values.ravel("K")))
    node_map = {node: i for i, node in enumerate(unique_nodes)}

    # Aggregate source and destination values for the Sankey chart
    df["value"] = df[value_col]

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=unique_nodes
        ),
        link=dict(
            source=df[source_col].map(node_map),
            target=df[target_col].map(node_map),
            value=df["value"]
        )
    )])
    return fig

if "preloaded_5" not in st.session_state:
    preloaded_5 = {}
    for i in day_list:
        date = today - timedelta(days=i)
        date = date.strftime('%Y-%m-%dT%H:%M:%S')
    
        data = sankey_data(date)
        preloaded_5[i] = data
    
    date = time_point['oldest_time'][0]
    data = sankey_data(date)
    preloaded_5[0] = data

    st.session_state["preloaded_5"] = preloaded_5

if time_ranges[selected_range_5] is not None:
    
    st.subheader("Asset Flow Chart")
    st.write("Flow Chart for the Top 10 Flows Between Assets")
    asset_chart = create_sankey_chart(
        st.session_state["preloaded_5"][time_ranges[selected_range_5]]['top_asset_data'], "source_id", "dest_id", "total_source_volume"
    )
    st.plotly_chart(asset_chart)
    
    st.subheader("Chain Flow Chart")
    st.write("Flow Chart for the Top 10 Flows Between Chains")
    chain_chart = create_sankey_chart(
        st.session_state["preloaded_5"][time_ranges[selected_range_5]]['top_chain_data'], "source_chain", "dest_chain", "total_source_volume"
    )
    st.plotly_chart(chain_chart)

    st.subheader("Asset-Chain Pair Flow Chart")
    st.write("Flow Chart for the Top 10 Flows Between Asset-Chain Pairs")
    pair_chart = create_sankey_chart(
        st.session_state["preloaded_5"][time_ranges[selected_range_5]]['top_pair_data'], 
        "source_pair", 
        "dest_pair", 
        "avg_volume"
    )
    st.plotly_chart(pair_chart)
    
else:
    
    st.subheader("Asset Flow Chart")
    st.write("Flow Chart for the Top 10 Flows Between Assets")
    asset_chart = create_sankey_chart(
        st.session_state["preloaded_5"][0]['top_asset_data'], "source_id", "dest_id", "total_source_volume"
    )
    st.plotly_chart(asset_chart)
    
    st.subheader("Chain Flow Chart")
    st.write("Flow Chart for the Top 10 Flows Between Chains")
    chain_chart = create_sankey_chart(
        st.session_state["preloaded_5"][0]['top_chain_data'], "source_chain", "dest_chain", "total_source_volume"
    )
    st.plotly_chart(chain_chart)

    st.subheader("Asset-Chain Pair Flow Chart")
    st.write("Flow Chart for the Top 10 Flows Between Asset-Chain Pairs")
    pair_chart = create_sankey_chart(
        st.session_state["preloaded_5"][0]['top_pair_data'], 
        "source_pair", 
        "dest_pair", 
        "avg_volume"
    )
    st.plotly_chart(pair_chart)


time_ranges_6 = {
    "All Time": None,  # Special case for no date filter
    "Last Week": 7,
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}

# Get today's date
today = datetime.now()

# Plotting the first chart (Chain Pair vs Median Fill Time) using Altair
st.title('Fill Time Visualizations')

selected_range_6 = st.selectbox("Select a time range for the fill time visualization:", list(time_ranges_6.keys()))

# Calculate the start date
if time_ranges_6[selected_range_6] is not None:
    start_date_6 = today - timedelta(days=time_ranges_6[selected_range_6])
    start_date_6 = start_date_6.strftime('%Y-%m-%dT%H:%M:%S')
    #st.write(start_date)
else:
    start_date_6 = time_point['oldest_time'][0]  # No filter for "All Time"
    #st.write(start_date)

@st.cache_data
def fill_time_gather(sd):

    # Supabase credentials
    supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6a2VmdGR6Z3NldWdpanBsaHNoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMjcxMzk3NCwiZXhwIjoyMDQ4Mjg5OTc0fQ.Og46ddAeoybqUavWBAUbUoj8HJiZrfAQZi-6gRP46i4"
    
    sql_query1 = f"""
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            cal.chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        WHERE op.block_timestamp >= '{sd}'
    ),
    fill_table AS (
      SELECT order_uuid, chain, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    ),
    median_time_fill_table AS (
        SELECT
            DATE(time_order_made) AS order_date,  -- Extract the date from time_order_made
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_time) AS median_fill_time
        FROM fill_table
        GROUP BY order_date  -- Group by the extracted date
        ORDER BY order_date
    )
    SELECT * 
    FROM median_time_fill_table
    """

    sql_query2 = f"""
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            cal.chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        WHERE op.block_timestamp >= '{sd}'
    ),
    fill_table AS (
      SELECT order_uuid, chain, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    ),
    median_source_chain_fill_table AS (
    SELECT 
        chain,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_time) AS median_fill_time
    FROM fill_table
    GROUP BY chain
    ORDER BY median_fill_time
    )
    SELECT * FROM median_source_chain_fill_table
    """

    sql_query6 = f"""
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            cal.chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.dest_asset = cal.address
        WHERE op.block_timestamp >= '{sd}'
    ),
    fill_table AS (
      SELECT order_uuid, chain, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    ),
    median_dest_chain_fill_table AS (
    SELECT 
        chain,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_time) AS median_fill_time
    FROM fill_table
    GROUP BY chain
    ORDER BY median_fill_time
    )
    SELECT * FROM median_dest_chain_fill_table
    """

    sql_query3 = f"""
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            cal.chain AS source_chain,
            cal2.chain AS dest_chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        INNER JOIN coingecko_assets_list cal2
          ON op.dest_asset = cal2.address
        WHERE op.block_timestamp >= '{sd}'
    ),
    fill_table AS (
      SELECT order_uuid, source_chain, dest_chain, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    ),
    median_chain_fill_table AS (
    SELECT 
        source_chain,
        dest_chain,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_time) AS median_fill_time
    FROM fill_table
    GROUP BY source_chain, dest_chain
    ORDER BY median_fill_time
    )
    SELECT * FROM median_chain_fill_table
    """

    sql_query4 = f"""
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            op.source_asset as source_address,
            op.dest_asset as dest_address,
            cal.chain AS source_chain,
            cal2.chain AS dest_chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        INNER JOIN coingecko_assets_list cal2
          ON op.dest_asset = cal2.address
        WHERE op.block_timestamp >= '{sd}'
    ),
    fill_table AS (
      SELECT order_uuid, source_chain, dest_chain, source_address, dest_address, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    )
    SELECT * 
    FROM fill_table
    ORDER BY fill_time DESC
    LIMIT 10
    """

    sql_query5 = f"""
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            op.source_asset as source_address,
            op.dest_asset as dest_address,
            cal.chain AS source_chain,
            cal2.chain AS dest_chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        INNER JOIN coingecko_assets_list cal2
          ON op.dest_asset = cal2.address
        WHERE op.block_timestamp >= '{sd}'
    ),
    fill_table AS (
      SELECT order_uuid, source_chain, dest_chain, source_address, dest_address, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    )
    SELECT * 
    FROM fill_table
    ORDER BY fill_time ASC
    LIMIT 10
    """

    # Call the function
    df_fill_time_date = execute_sql(sql_query1)
    df_fill_time_s_chain = execute_sql(sql_query2)
    df_fill_time_chain = execute_sql(sql_query3)
    df_fill_time_highest = execute_sql(sql_query4)
    df_fill_time_lowest = execute_sql(sql_query5)
    df_fill_time_d_chain = execute_sql(sql_query6)

    df_fill_time_date = pd.json_normalize(df_fill_time_date['result'])
    df_fill_time_s_chain = pd.json_normalize(df_fill_time_s_chain['result'])
    df_fill_time_d_chain = pd.json_normalize(df_fill_time_d_chain['result'])
    df_fill_time_chain = pd.json_normalize(df_fill_time_chain['result'])
    df_fill_time_highest = pd.json_normalize(df_fill_time_highest['result'])
    df_fill_time_lowest = pd.json_normalize(df_fill_time_lowest['result'])


    # Create chain pair column for better visualization
    df_fill_time_chain['chain_pair'] = df_fill_time_chain['source_chain'] + ' to ' + df_fill_time_chain['dest_chain']

    # Sorting the median fill times in descending order for better visualization
    df_fill_time_chain = df_fill_time_chain.sort_values(by='median_fill_time', ascending=False)

    df_fill_time_s_chain_sorted = df_fill_time_s_chain.sort_values(by='median_fill_time', ascending=False)
    df_fill_time_s_chain_sorted = df_fill_time_s_chain_sorted.reset_index(drop=True)
    s_chain_index = df_fill_time_s_chain_sorted.index
    
    if s_chain_index[0] == 0:
        df_fill_time_s_chain_sorted.index = df_fill_time_s_chain_sorted.index+1
    # Display the sorted table
    #df_fill_time_s_chain_sorted = df_fill_time_s_chain_sorted.rename(
    #    columns={
    #        'chain': 'Chain',
    #        'median_fill_time': 'Median Fill Time'
    #    }
    #)
    df_fill_time_s_chain_sorted_2 = df_fill_time_s_chain_sorted[['chain', 'median_fill_time']]

    df_fill_time_s_chain_sorted_2 = df_fill_time_s_chain_sorted_2.rename(
        columns={
            'chain': 'Chain',
            'median_fill_time': 'Median Fill Time'
        }
    )
    
    df_fill_time_d_chain_sorted = df_fill_time_d_chain.sort_values(by='median_fill_time', ascending=False)
    df_fill_time_d_chain_sorted = df_fill_time_d_chain_sorted.reset_index(drop=True)
    d_chain_index = df_fill_time_d_chain_sorted.index
    
    if d_chain_index[0] == 0:
        df_fill_time_d_chain_sorted.index = df_fill_time_d_chain_sorted.index+1
    # Display the sorted table
    #df_fill_time_s_chain_sorted = df_fill_time_s_chain_sorted.rename(
    #    columns={
    #        'chain': 'Chain',
    #        'median_fill_time': 'Median Fill Time'
    #    }
    #)
    df_fill_time_d_chain_sorted_2 = df_fill_time_d_chain_sorted[['chain', 'median_fill_time']]

    df_fill_time_d_chain_sorted_2 = df_fill_time_d_chain_sorted_2.rename(
        columns={
            'chain': 'Chain',
            'median_fill_time': 'Median Fill Time'
        }
    )

    df_fill_time_lowest_reform = df_fill_time_lowest[['order_uuid', 'source_chain', 'dest_chain', 'source_address', 'dest_address', 'time_order_made', 'fill_time']]
    df_fill_time_lowest_reform = df_fill_time_lowest_reform.rename(columns={
        'order_uuid': 'Order ID',
        'source_chain': 'Source Chain',
        'dest_chain': 'Destination Chain',
        'source_address': 'Source Address',
        'dest_address': 'Destination Address',
        'time_order_made': 'Time',
        'fill_time': 'Fill Time'
    })
    fill_time_lowest_reform_index = df_fill_time_lowest_reform.index 
    
    if fill_time_lowest_reform_index[0] == 0:
        df_fill_time_lowest_reform.index = df_fill_time_lowest_reform.index + 1

    df_fill_time_highest_reform = df_fill_time_highest[['order_uuid', 'source_chain', 'dest_chain', 'source_address', 'dest_address', 'time_order_made', 'fill_time']]
    df_fill_time_highest_reform = df_fill_time_highest_reform.rename(columns={
        'order_uuid': 'Order ID',
        'source_chain': 'Source Chain',
        'dest_chain': 'Destination Chain',
        'source_address': 'Source Address',
        'dest_address': 'Destination Address',
        'time_order_made': 'Time',
        'fill_time': 'Fill Time'
    })
    #st.dataframe(df_fill_time_highest[['order_uuid', 'source_chain', 'dest_chain', 'source_address', 'dest_address', 'time_order_made', 'fill_time']])
    fill_time_highest_reform_index = df_fill_time_highest_reform.index
    if fill_time_highest_reform_index[0] == 0:
        df_fill_time_highest_reform.index = df_fill_time_highest_reform.index + 1
    
    return {
            "df_fill_time_date": df_fill_time_date,
            "df_fill_time_s_chain": df_fill_time_s_chain,
            "df_fill_time_d_chain": df_fill_time_d_chain,
            "df_fill_time_chain": df_fill_time_chain,
            "df_fill_time_highest": df_fill_time_highest,
            "df_fill_time_lowest": df_fill_time_lowest,
            "df_fill_time_s_chain_sorted_2": df_fill_time_s_chain_sorted_2,
            "df_fill_time_d_chain_sorted_2": df_fill_time_d_chain_sorted_2,
            "df_fill_time_lowest_reform": df_fill_time_lowest_reform,
            "df_fill_time_highest_reform": df_fill_time_highest_reform,
        }

#    st.set_page_config(layout="wide")

def fill_time_builds(load):
    # Create two columns to place the charts next to each other

    df_fill_time_date = load['df_fill_time_date']
    df_fill_time_s_chain = load['df_fill_time_s_chain']
    df_fill_time_d_chain = load['df_fill_time_d_chain']
    df_fill_time_chain = load['df_fill_time_chain']
    df_fill_time_highest = load['df_fill_time_highest']
    df_fill_time_lowest = load['df_fill_time_lowest']
    df_fill_time_s_chain_sorted_2 = load['df_fill_time_s_chain_sorted_2']
    df_fill_time_d_chain_sorted_2 = load['df_fill_time_d_chain_sorted_2']
    df_fill_time_lowest_reform = load['df_fill_time_lowest_reform']
    df_fill_time_highest_reform = load['df_fill_time_highest_reform']
    # First chart (Chain Pair vs Median Fill Time)

    col1, col2, col3, col4 = st.columns([4, 4, 2, 2])
    with col1:
        st.subheader('Median Fill Time by Chain Pair')
        chart_chain = alt.Chart(df_fill_time_chain).mark_bar().encode(
            x=alt.X('chain_pair:N', sort=None),  # Chain pair on x-axis
            y='median_fill_time:Q',  # Median fill time on y-axis
            color='median_fill_time:Q',  # Color by median fill time
            tooltip=['chain_pair:N', 'median_fill_time:Q']  # Tooltip with chain pair and median fill time
        )
        st.altair_chart(chart_chain, use_container_width=True)

    # Second chart (Fill Time by Date) as a line chart
    with col2:
        st.subheader('Median Fill Time by Date')
        # Convert 'date' column to datetime for line chart
        df_fill_time_date['order_date'] = pd.to_datetime(df_fill_time_date['order_date'])

        # Display the line chart using Streamlit's st.line_chart
        st.line_chart(df_fill_time_date.set_index('order_date')['median_fill_time'])
        
    # Third container (Table) displaying df_fill_time_s_chain, sorted by median_fill_time
    with col3:
        st.subheader('Source Chain Median Fill Time')
        # Sort df_fill_time_s_chain by median_fill_time in descending order
        
        st.dataframe(df_fill_time_s_chain_sorted_2)

    with col4:
        st.subheader('Destination Chain Median Fill Time')
        # Sort df_fill_time_s_chain by median_fill_time in descending order
        
        st.dataframe(df_fill_time_d_chain_sorted_2)
        

    # Centering the dataframe using columns
    col1, col2, col3 = st.columns([0.5, 7, 0.5])  # Use a ratio of 1:2:1 to center the dataframe
    
    with col2:  # This column will be centered
        st.subheader("Orders with the Ten Lowest Fill Times")
    
        #st.dataframe(df_fill_time_lowest[['order_uuid', 'source_chain', 'dest_chain', 'source_address', 'dest_address', 'time_order_made', 'fill_time']])
        st.dataframe(df_fill_time_lowest_reform)   
    
    # Centering the dataframe using columns
    col1, col2, col3 = st.columns([0.5, 7, 0.5])  # Use a ratio of 1:2:1 to center the dataframe
    
    with col2:  # This column will be centered
        st.subheader("Orders with the Ten Highest Fill Times")
        
        st.dataframe(df_fill_time_highest_reform)   

    return

if "preloaded_6" not in st.session_state:
    preloaded_6 = {}
    for i in day_list:
        date = today - timedelta(days=i)
        date = date.strftime('%Y-%m-%dT%H:%M:%S')
    
        data = fill_time_gather(date)
        preloaded_6[i] = data
    
    date = time_point['oldest_time'][0]
    data = fill_time_gather(date)
    preloaded_6[0] = data

    st.session_state["preloaded_6"] = preloaded_6

if time_ranges[selected_range_6] is not None:
    fill_time_builds(st.session_state["preloaded_6"][time_ranges[selected_range_6]])
else:
    fill_time_builds(st.session_state["preloaded_6"][0])



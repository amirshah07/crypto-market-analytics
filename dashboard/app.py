import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Crypto Market Analytics",
    page_icon="💰",
    layout="wide"
    )

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="crypto_analytics",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def get_top_gainers():
    conn = get_db_connection()
    query = """
        SELECT 
            c.name,
            c.symbol,
            cp.price_usd,
            cp.price_change_pct_24h,
            cp.market_cap
        FROM crypto_prices AS cp
        INNER JOIN coins AS c 
        ON cp.coin_key = c.coin_key
        WHERE cp.time_key = (
            SELECT MAX(time_key) 
            FROM crypto_prices
        )
        ORDER BY cp.price_change_pct_24h DESC
        LIMIT 10;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_top_losers():
    conn = get_db_connection()
    query = """
        SELECT 
            c.name,
            c.symbol,
            cp.price_usd,
            cp.price_change_pct_24h,
            cp.market_cap
        FROM crypto_prices AS cp
        INNER JOIN coins AS c 
        ON cp.coin_key = c.coin_key
        WHERE cp.time_key = (
            SELECT MAX(time_key) 
            FROM crypto_prices
        )
        ORDER BY cp.price_change_pct_24h ASC
        LIMIT 10;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_price_history(coin_id):
    conn = get_db_connection()
    query = f"""
        SELECT 
            c.name,
            c.symbol,
            cp.price_usd,
            cp.market_cap,
            cp.price_change_pct_24h,
            ti.timestamp
        FROM crypto_prices AS cp
        INNER JOIN coins AS c 
        ON cp.coin_key = c.coin_key
        INNER JOIN time_intervals AS ti
        ON cp.time_key = ti.time_key
        WHERE c.coin_id = '{coin_id}'
        ORDER BY ti.timestamp ASC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_market_cap_rankings():
    conn = get_db_connection()
    query = """
        SELECT 
            ROW_NUMBER() OVER (ORDER BY cp.market_cap DESC) AS rank,
            c.coin_id, 
            c.name, 
            c.symbol, 
            cp.price_usd, 
            cp.market_cap, 
            cp.price_change_pct_24h, 
            ti.timestamp AS last_updated
        FROM crypto_prices AS cp
        INNER JOIN coins AS c 
        ON cp.coin_key = c.coin_key
        INNER JOIN time_intervals AS ti 
        ON cp.time_key = ti.time_key
        WHERE cp.time_key = (
            SELECT MAX(time_key)
            FROM crypto_prices
            WHERE coin_key = cp.coin_key
        )
        ORDER BY cp.market_cap DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_all_coins():
    conn = get_db_connection()
    query = "SELECT coin_id, name FROM coins ORDER BY name;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def format_large_number(num):
    if num >= 1000000000000:
        return f"${num/1000000000000:.2f}T"
    elif num >= 1000000000:
        return f"${num/1000000000:.2f}B"
    elif num >= 1000000:
        return f"${num/1000000:.2f}M"
    elif num >= 1000:
        return f"${num/1000:.2f}K"
    else:
        return f"${num:.2f}"

def main():
    with st.sidebar:
        st.title("Pages")
        page = st.radio(
            "",
            ["Top Movers", "Price History", "Market Cap Rankings"],
            label_visibility="collapsed"
        )

    if page == "Top Movers":
        show_top_movers()
    elif page == "Price History":
        show_price_history()
    elif page == "Market Cap Rankings":
        show_market_cap_rankings()


def show_top_movers():
    st.header("Top Movers (24h)")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 10 Gainers")
        gainers = get_top_gainers()

        fig = px.bar(
            gainers,
            x="price_change_pct_24h",
            y="name",
            orientation="h",
            labels={"price_change_pct_24h": "24h Change (%)", "name": ""},
            color="price_change_pct_24h",
            color_continuous_scale="Greens",
            text="price_change_pct_24h"
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(showlegend=False, height=500,
                          xaxis=dict(showgrid=False),
                          yaxis=dict(showgrid=False))
        st.plotly_chart(fig, use_container_width=True)

        display_gainers = gainers.copy()
        display_gainers["price_usd"] = display_gainers["price_usd"].apply(lambda x: f"${x:,.2f}")
        display_gainers["market_cap"] = display_gainers["market_cap"].apply(format_large_number)
        display_gainers["price_change_pct_24h"] = display_gainers["price_change_pct_24h"].apply(lambda x: f"+{x:.2f}%")
        st.dataframe(display_gainers, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Top 10 Losers")
        losers = get_top_losers()

        fig = px.bar(
            losers,
            x="price_change_pct_24h",
            y="name",
            orientation="h",
            labels={"price_change_pct_24h": "24h Change (%)", "name": ""},
            color="price_change_pct_24h",
            color_continuous_scale="Reds_r",
            text="price_change_pct_24h"
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(showlegend=False, height=500,
                          xaxis=dict(showgrid=False),
                          yaxis=dict(showgrid=False))
        st.plotly_chart(fig, use_container_width=True)

        display_losers = losers.copy()
        display_losers["price_usd"] = display_losers["price_usd"].apply(lambda x: f"${x:,.2f}")
        display_losers["market_cap"] = display_losers["market_cap"].apply(format_large_number)
        display_losers["price_change_pct_24h"] = display_losers["price_change_pct_24h"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(display_losers, use_container_width=True, hide_index=True)


def show_price_history():
    st.header("Price History")

    coins = get_all_coins()
    coin_options = {
        f"{row['name']} ({row['coin_id']})": row["coin_id"]
        for _, row in coins.iterrows()
    }

    selected_coin_display = st.selectbox(
        "Select a cryptocurrency",
        list(coin_options.keys()),
        index=list(coin_options.values()).index("bitcoin")
    )
    selected_coin = coin_options[selected_coin_display]

    st.markdown("###")

    df = get_price_history(selected_coin)

    if df.empty:
        st.warning("No historical data available for this coin.")
        return

    current_price = df.iloc[-1]["price_usd"]
    price_change = df.iloc[-1]["price_change_pct_24h"]
    market_cap = df.iloc[-1]["market_cap"]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Current Price", f"${current_price:,.2f}")
    col2.metric("24h Change", f"{price_change:.2f}%")
    col3.metric("Market Cap", format_large_number(market_cap))
    col4.metric("Data Points", f"{len(df)}")

    st.markdown("###")

    fig = px.line(
        df,
        x="timestamp",
        y="price_usd",
        labels={"timestamp": "Date & Time", "price_usd": "Price (USD)"},
        title=f"{df.iloc[0]['name']} Price History"
    )
    fig.update_traces(
        line_color="#00ff00",
        line_width=2,
        hovertemplate="<b>%{y:$,.2f}</b><br>%{x}<extra></extra>"
    )
    fig.update_layout(
        hovermode="x unified",
        height=500,
        xaxis=dict(showgrid=True, gridcolor="#333"),
        yaxis=dict(showgrid=True, gridcolor="#333")
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Historical Data")

    display_df = df.copy()
    display_df["price_usd"] = display_df["price_usd"].apply(lambda x: f"${x:,.2f}")
    display_df["market_cap"] = display_df["market_cap"].apply(format_large_number)
    display_df["price_change_pct_24h"] = display_df["price_change_pct_24h"].apply(lambda x: f"{x:.2f}%")
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)


def show_market_cap_rankings():
    st.header("Market Cap Rankings")

    df = get_market_cap_rankings()

    st.subheader("Top 10 by Market Cap")
    top_10 = df.head(10)

    fig = px.bar(
        top_10,
        x="market_cap",
        y="name",
        orientation="h",
        labels={"market_cap": "Market Cap (USD)", "name": ""},
        color="market_cap",
        color_continuous_scale="Blues",
        text="market_cap"
    )
    fig.update_traces(
        texttemplate="%{text:,.0f}",
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>$%{x:,.0f}<extra></extra>"
    )
    fig.update_layout(
        showlegend=False,
        height=500,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False, autorange="reversed")
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("###")
    st.subheader("Complete Rankings")

    display_df = df.copy()
    display_df["price_usd"] = display_df["price_usd"].apply(lambda x: f"${x:,.2f}")
    display_df["market_cap"] = display_df["market_cap"].apply(format_large_number)
    display_df["price_change_pct_24h"] = display_df["price_change_pct_24h"].apply(lambda x: f"{x:.2f}%")
    display_df = display_df.rename(columns={
        "rank": "Rank",
        "coin_id": "Coin ID",
        "name": "Name",
        "symbol": "Symbol",
        "price_usd": "Price (USD)",
        "market_cap": "Market Cap",
        "price_change_pct_24h": "24h Change",
        "last_updated": "Last Updated"
    })

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)


if __name__ == "__main__":
    main()
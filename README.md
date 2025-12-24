# Crypto Market Analytics

Cryptocurrency market analytics with automated data pipeline, star schema data warehouse, and interactive dashboard.

## 1. How it works

1. **Data Collection**: Python script fetches market data for the top 50 cryptocurrencies by market cap from CoinGecko API every 5 minutes
2. **Raw Storage**: Market data stored as timestamped CSV files in `data/` directory
3. **ETL Processing**: PySpark reads all CSV files, transforms data, and prepares for database loading
4. **Data Warehouse**: New records inserted into PostgreSQL star schema (dimension tables: coins, time_intervals; fact table: crypto_prices)
5. **Visualisation**: Streamlit dashboard queries PostgreSQL and renders interactive charts with Plotly

## 2. Code overview
```
crypto-market-analytics/
├── collectors/
│   └── coingecko_collector.py     # CoinGecko API data collector
├── dashboard/
│   └── app.py                     # Streamlit dashboard with 3 pages
├── data/                          # CSV files (gitignored)
├── spark_jobs/
│   └── crypto_etl_pipeline.py     # PySpark ETL
├── sql/
│   ├── analytics.sql              # Reference queries
│   └── create_tables.sql          # Database schema
├── docker-compose.yml             # PostgreSQL container
└── requirements.txt
```

## 3. Database schema

**Dimension Tables:**
- `coins`: coin_key (PK), coin_id, symbol, name
- `time_intervals`: time_key (PK), timestamp, date, hour, minute, day, month, year

**Fact Table:**
- `crypto_prices`: price_id (PK), coin_key (FK), time_key (FK), price_usd, market_cap, volume_24h, price_change_pct_24h, collected_at

## 4. Setup

**Prerequisites:**
- Python 3.10+
- Docker
- Git

**Installation:**
```bash
# Clone repository
git clone https://github.com/amirshah07/crypto-market-analytics.git
cd crypto-market-analytics

# Create virtual environment
python -m venv venv
source venv/bin/activate 

# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL
docker-compose up -d
```

**Usage:**
```bash
# 1. Collect data (runs every 5 minutes when active)
python collectors/coingecko_collector.py

# 2. Run ETL pipeline
python spark_jobs/crypto_etl_pipeline.py

# 3. Launch dashboard
streamlit run dashboard/app.py
```

**Example ETL output:**
```
Inserted 0 new coins
Inserted 1 new time intervals
Inserted 50 new price records
ETL pipeline completed successfully!
```

## 5. Dashboard Pages

1. **Top Movers (24h)**: Side-by-side green/red bar charts showing top 10 gainers and losers
2. **Price History**: Interactive time-series line chart with cryptocurrency selector dropdown
3. **Market Cap Rankings**: Horizontal bar chart and complete table ranked by market capitalisation
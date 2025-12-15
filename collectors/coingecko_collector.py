from datetime import datetime
import time
import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()
url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&price_change_percentage=24h"
headers = {"x-cg-api-key": os.getenv("COINGECKO_API_KEY")}

consecutive_failures = 0 # count in case api down then break

while True: # update to cron job in the future
    if consecutive_failures == 10: # api down for 50 minutes, break
        break

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        consecutive_failures = 0
        data = response.json()
        collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        filtered_data = [
            {
                "id": coin["id"],
                "symbol": coin["symbol"],
                "name": coin["name"],
                "current_price": coin["current_price"],
                "market_cap": coin["market_cap"],
                "total_volume": coin["total_volume"],
                "price_change_percentage_24h": coin["price_change_percentage_24h"],
                "last_updated": coin["last_updated"],
                "collected_at": collected_at
            }
            for coin in data
        ]

        df = pd.DataFrame(filtered_data)
        now = datetime.now()
        df.to_csv(f"data/crypto_data_{now.day:02d}-{now.month:02d}-{now.year}_{now.hour:02d}{now.minute:02d}.csv", index=False)
        print("Success, CSV created!")
        time.sleep(300) # 5 mins delay between calls

    else:
        consecutive_failures += 1
        print("Error:", response.status_code)
        time.sleep(300) # 5 mins delay between calls
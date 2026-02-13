import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# ---------------- CONFIG ---------------- #
load_dotenv()

API_URL = os.getenv("EXCHANGE_KEY", "").strip()
DB_URL = os.getenv("SUPABASE_URL", "").strip()

# --- FIX: Connection Resilience ---
# 1. Force SSL: Supabase requires it.
# 2. Use a connection string that handles IPv4 properly.
if "sslmode" not in DB_URL:
    separator = "&" if "?" in DB_URL else "?"
    DB_URL += f"{separator}sslmode=require"

# Create engine with a longer timeout and pre-ping to handle network jitter
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    connect_args={
        "connect_timeout": 10,  # Give it a bit more time to handshake
    },
)


def capture_historical_rates():
    # 1. Fetch Data
    try:
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        data = response.json()

        rates_dict = data.get("conversion_rates", {})
        api_time = data.get("time_last_update_utc")

        if not rates_dict or not api_time:
            print("❌ Data missing in API response.")
            return

    except Exception as e:
        print(f"❌ API Error: {e}")
        return

    # 2. Vectorized Transformation
    df = pd.DataFrame(list(rates_dict.items()), columns=["currency_code", "rate"])
    df["recorded_at"] = pd.to_datetime(api_time)

    # 3. Load into PostgreSQL
    try:
        with engine.begin() as connection:
            df.to_sql(
                name="exchange_rates",
                con=connection,
                if_exists="append",
                index=False,
                chunksize=500,
                method="multi",
            )
        print(f"✅ Successfully saved {len(df)} rates (Timestamp: {api_time})")

    except Exception as e:
        print(f"❌ Database Error: {e}")


if __name__ == "__main__":
    capture_historical_rates()

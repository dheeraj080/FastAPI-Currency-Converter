import os
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- CONFIG ---
load_dotenv()

API_URL = os.getenv("EXCHANGE_KEY", "").strip()
DB_URL = os.getenv("SUPABASE_URL", "").strip()

if not API_URL or not DB_URL:
    logger.error("Missing environment variables: EXCHANGE_KEY or SUPABASE_URL")
    exit(1)

# Ensure SSL for Supabase
if "sslmode" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"

# Optimized engine for Cloud/Serverless environments
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_recycle=300,  # Refresh connections before Supabase kills them
    connect_args={"connect_timeout": 10},
)


def capture_historical_rates():
    # 1. Fetch Data
    try:
        logger.info("Fetching data from API...")
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        data = response.json()

        rates_dict = data.get("conversion_rates", {})
        api_time = data.get("time_last_update_utc")

        if not rates_dict or not api_time:
            logger.error("Data missing in API response.")
            return

    except requests.exceptions.RequestException as e:
        logger.error(f"API Connection Error: {e}")
        return

    # 2. Transformation
    # Convert to DataFrame and cast types explicitly
    df = pd.DataFrame(list(rates_dict.items()), columns=["currency_code", "rate"])
    df["recorded_at"] = pd.to_datetime(api_time)
    df["rate"] = pd.to_numeric(df["rate"])

    # 3. Load into PostgreSQL with Idempotency Check
    try:
        with engine.begin() as conn:
            # OPTIONAL: Check if data for this timestamp already exists to avoid duplicates
            check_sql = text(
                "SELECT EXISTS(SELECT 1 FROM exchange_rates WHERE recorded_at = :t LIMIT 1)"
            )
            exists = conn.execute(check_sql, {"t": df["recorded_at"].iloc[0]}).scalar()

            if exists:
                logger.warning(f"Data for {api_time} already exists. Skipping upload.")
                return

            # Bulk Insert
            df.to_sql(
                name="exchange_rates",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )

        logger.info(f"✅ Saved {len(df)} rates for {api_time}")

    except Exception as e:
        logger.error(f"Database Error: {e}")


if __name__ == "__main__":
    capture_historical_rates()

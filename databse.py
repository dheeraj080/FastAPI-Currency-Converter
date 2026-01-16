import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('SUPABASE_URL')

# Check if URL exists to avoid confusing errors later
if not DATABASE_URL:
    raise ValueError("SUPABASE_URL not found in environment variables")

# Use a pool size to handle multiple users simultaneously
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def check_db_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ DB CONNECTED")
    except Exception as e:
        print(f"❌ DB FAILED: {e}")
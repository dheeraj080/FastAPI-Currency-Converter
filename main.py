import uvicorn
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

DATABASE_URL = (os.getenv('SUPABASE_URL'))
#
try:
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()
    print("DB CONNECTED")
    connection.close()
except:
    print("DB FAILED")

amount = float(input("Enter amount to convert: "))
from_currerncy = input("Enter first code: ").upper()
to_currency = input("Enter second code: ").upper()

try:
    with engine.connect() as connection:
        # 1. Define the query with named parameters
        query = text("SELECT currency_codes, rate FROM exchange_rates WHERE currency_codes IN (:c1, :c2)")
        result = connection.execute(query, {"c1": from_currerncy, "c2": to_currency})
        # 2. Pass the dictionary as the second argument
        # This resolves the 'InvalidRequestError'
        
        rates = {row.currency_codes: row.rate for row in result}
        
        if from_currerncy in rates and to_currency in rates:
            rate_from = rates[from_currerncy]
            rate_to = rates[to_currency]
            
            converted_amount = amount * (rate_to/rate_from)
            
            print(f"---Conversion---")
            print(f"{amount}{from_currerncy} = {converted_amount:.2f}{to_currency}")
        
        for row in result:
            print(f"Code: {row.currency_codes}, Rate: {row.rate}")

except Exception as e:
    print("Error:", e)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
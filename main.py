import uvicorn
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

DATABASE_URL = (os.getenv('SUPABASE_URL'))

try:
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()
    print("DB CONNECTED")
    connection.close()
except:
    print("DB FAILED")
  
@app.get("/convert")
def convert(from_currency:str, to_currency:str, amount:float):
    
    try:
        with engine.connect() as connection:
            query = text("SELECT currency_codes, rate FROM exchange_rates WHERE currency_codes IN (:c1, :c2)")
            result = connection.execute(query, {"c1": from_currency, "c2": to_currency})
            
            rates = {row.currency_codes: row.rate for row in result}
        
        if from_currency not in rates or to_currency not in rates:
            raise HTTPException(status_code=404, detail="One or both currency codes not found")
    
        rate_from = rates[from_currency]
        rate_to = rates[to_currency]
            
        converted_amount = amount * (rate_to/rate_from)
        
        return {
            "amount": amount,
            "from": from_currency,
            "to": to_currency,
            "result": round(converted_amount, 2),
            "rate": round(rate_to / rate_from, 4)
        }
    
    
    
    except Exception as e:
        print("Error:", e)
        
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

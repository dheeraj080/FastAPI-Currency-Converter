import uvicorn
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from decimal import Decimal
from databse import get_connection


app = FastAPI()

@app.get("/convert")
def convert(
    from_currency:str= Query(..., min_length=3, max_length=3), 
    to_currency:str= Query(..., min_length=3, max_length=3), 
    amount:Decimal = Query(..., gt=0),
):
    
    try:
        with get_connection() as connection:
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
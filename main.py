import uvicorn
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from decimal import Decimal
from database import engine, check_db_connection

app = FastAPI(title="Currency Exchange API")

# Run a connection check on startup
@app.on_event("startup")
def startup_event():
    check_db_connection()

@app.get("/convert")
def convert(
    from_currency: str = Query(..., min_length=3, max_length=3, description="Source currency code"), 
    to_currency: str = Query(..., min_length=3, max_length=3, description="Target currency code"), 
    amount: Decimal = Query(..., gt=0)
):
    # 1. Normalize inputs to uppercase
    c1, c2 = from_currency.upper(), to_currency.upper()
    
    try:
        with engine.connect() as connection:
            # 2. Optimized SQL
            query = text("SELECT currency_codes, rate FROM exchange_rates WHERE currency_codes IN (:c1, :c2)")
            result = connection.execute(query, {"c1": c1, "c2": c2})
            
            # 3. Use dictionary for mapping
            rates = {row.currency_codes: Decimal(str(row.rate)) for row in result}
        
        # 4. Logical Validation
        if c1 not in rates or c2 not in rates:
            missing = []
            if c1 not in rates: missing.append(c1)
            if c2 not in rates: missing.append(c2)
            raise HTTPException(status_code=404, detail=f"Currency codes not found: {', '.join(missing)}")

        # 5. Calculation using Decimal for precision
        rate_from = rates[c1]
        rate_to = rates[c2]
        converted_amount = amount * (rate_to / rate_from)
        
        return {
            "amount": amount,
            "from": c1,
            "to": c2,
            "result": round(converted_amount, 2),
            "rate": round(rate_to / rate_from, 4)
        }
    
    except HTTPException:
        # Re-raise FastAPI errors so they aren't caught by the general Exception block
        raise
    except Exception as e:
        # Log the real error to the console, but send a generic one to the user
        print(f"Internal Server Error: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred.")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
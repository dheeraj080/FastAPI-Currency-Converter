import uvicorn
from decimal import Decimal
from fastapi import FastAPI, Query, Depends
from service import ExchangeRateService

app = FastAPI(title="Currency Exchange API")


@app.get("/convert")
def convert(
    from_currency: str = Query(
        ..., min_length=3, max_length=3, description="Source currency code"
    ),
    to_currency: str = Query(
        ..., min_length=3, max_length=3, description="Target currency code"
    ),
    amount: Decimal = Query(..., gt=0),
    service: ExchangeRateService = Depends(ExchangeRateService),
):
    result = service.convert(from_currency, to_currency, amount)
    return result


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

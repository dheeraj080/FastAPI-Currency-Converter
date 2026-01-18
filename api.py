from decimal import Decimal
from fastapi import FastAPI, Depends, Query, APIRouter, Request
from service2 import ExchangeRateService
from slowapi import Limiter
from slowapi.util import get_remote_address
from datetime import datetime, timezone

router = APIRouter()
Limiter = Limiter(key_func=get_remote_address)


@Limiter.limit("30/minute")
@router.get("/latest")
def convert(
    request: Request,
    from_currency: str = Query(..., min_length=1, max_length=10),
    to_currency: str = Query(..., min_length=1, max_length=10),
    amount: Decimal = Query(..., gt=0),
    service: ExchangeRateService = Depends(ExchangeRateService),
):
    result = service.convert(from_currency, to_currency, amount)
    return result


"""
@Limiter.limit("30/minute")
@router.get("/ondate")'''''
def convert_historical(
    request: Request,
    from_currency: str = Query(..., min_length=3, max_length=3),
    to_currency: str = Query(..., min_length=3, max_length=3),
    amount: Decimal = Query(..., gt=0),
    date: datetime = Query(...),
    service: ExchangeRateService = Depends(ExchangeRateService),
):
    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    result = service.convert_historical(from_currency, to_currency, amount, date)
    return result
"""


@router.get("/health")
def health():
    return {"status": "OK"}

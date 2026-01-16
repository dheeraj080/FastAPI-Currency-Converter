from decimal import Decimal
from fastapi import FastAPI, Depends, Query, APIRouter, Request
from service import ExchangeRateService
from slowapi import Limiter
from slowapi.util import get_remote_address

router = APIRouter()
Limiter = Limiter(key_func=get_remote_address)


@router.get("/convert")
@Limiter.limit("20/minute")
def convert(
    request: Request,
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


@router.get("/health")
def health():
    return {"status": "OK"}

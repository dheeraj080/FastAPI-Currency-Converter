from decimal import Decimal
from fastapi import FastAPI, Depends, Query, APIRouter
from service import ExchangeRateService

router = APIRouter()


@router.get("/convert")
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

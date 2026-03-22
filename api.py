import logging
from decimal import Decimal
from fastapi import FastAPI, Depends, Query, APIRouter, Request, HTTPException
from service2 import ExchangeRateService
from slowapi import Limiter
from slowapi.util import get_remote_address
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

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
    try:
        logger.info(f"Conversion request received: {amount} {from_currency} -> {to_currency}")
        result = service.convert(from_currency, to_currency, amount)
        logger.info(f"Conversion successful: {result['result']} {to_currency}")
        return result
    except ValueError as e:
        logger.warning(f"Client error during conversion: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected server error during conversion: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/health")
def health():
    return {"status": "OK"}

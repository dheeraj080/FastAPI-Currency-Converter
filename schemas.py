from pydantic import BaseModel
from decimal import Decimal

class ExchangeResponse(BaseModel):
    amount: Decimal
    from_currency: str
    to_currency: str
    result: Decimal
    rate: Decimal

    class Config:
        from_attributes = True
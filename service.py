from decimal import Decimal
from sqlalchemy import text
from database import engine


class ExchangeRateService:
    def convert(self, from_currency: str, to_currency: str, amount: Decimal) -> dict:
        # 1. Normalize inputs
        c1, c2 = from_currency.upper(), to_currency.upper()

        # 2. Database interaction
        with engine.connect() as connection:
            query = text(
                "SELECT currency_codes, rate FROM exchange_rates WHERE currency_codes IN (:c1, :c2)"
            )
            result = connection.execute(query, {"c1": c1, "c2": c2})

            # Map results to Decimal
            rates = {row.currency_codes: Decimal(str(row.rate)) for row in result}

        # 3. Validation logic
        if c1 not in rates or c2 not in rates:
            missing = [c for c in [c1, c2] if c not in rates]
            # We raise a ValueError here; main.py will catch this and turn it into an HTTPException
            raise ValueError(f"Currency codes not found: {', '.join(missing)}")

        # 4. Calculation
        rate_from = rates[c1]
        rate_to = rates[c2]

        # Calculate cross-rate
        conversion_rate = rate_to / rate_from
        converted_amount = amount * conversion_rate

        # 5. Return the full data dictionary
        return {
            "amount": amount,
            "from": c1,
            "to": c2,
            "result": round(converted_amount, 2),
            "rate": round(conversion_rate, 4),
        }

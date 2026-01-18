from decimal import Decimal
from sqlalchemy import text
from database import engine
from datetime import datetime, timezone


class ExchangeRateService:

    def convert(self, from_currency: str, to_currency: str, amount: Decimal) -> dict:
        # 1. Normalize inputs
        c1, c2 = from_currency.upper(), to_currency.upper()

        # 2. Database interaction
        with engine.connect() as connection:
            query = text(
                """
                SELECT DISTINCT ON (currency_code) currency_code, rate, recorded_at
                FROM exchange_rates
                WHERE currency_code IN (:c1, :c2)
                ORDER BY currency_code, recorded_at DESC
            """
            )
            result = connection.execute(query, {"c1": c1, "c2": c2})

            # Map results to Decimal
            rates = {
                row.currency_code: {
                    "rate": Decimal(str(row.rate)),
                    "timestamp": row.recorded_at,
                }
                for row in result
            }
        # 3. Validation logic
        if c1 not in rates or c2 not in rates:
            missing = [c for c in [c1, c2] if c not in rates]
            # We raise a ValueError here; main.py will catch this and turn it into an HTTPException
            raise ValueError(f"Currency codes not found: {', '.join(missing)}")

        # 4. Calculation
        rate_from = rates[c1]["rate"]
        rate_to = rates[c2]["rate"]

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
            "last_updated": rates[c1]["timestamp"],
        }

    def convert_historical(
        self,
        from_currency: str,
        to_currency: str,
        amount: Decimal,
        recorded_at: datetime,
    ) -> dict:
        c1, c2 = from_currency.upper(), to_currency.upper()

        with engine.connect() as connection:
            query = text(
                """
                SELECT DISTINCT ON (currency_code) currency_code, rate, recorded_at
                FROM exchange_rates
                WHERE currency_code IN (:c1, :c2) 
                  AND recorded_at <= :recorded_at
                ORDER BY currency_code, recorded_at DESC
            """
            )

            result = connection.execute(
                query, {"c1": c1, "c2": c2, "recorded_at": recorded_at}
            )

            rates = {
                row.currency_code: {
                    "rate": Decimal(str(row.rate)),
                    "timestamp": row.recorded_at,
                }
                for row in result
            }

        if c1 not in rates or c2 not in rates:
            raise ValueError(
                f"Historical rates for {c1}/{c2} not found for date {recorded_at}"
            )

        # Calculation Logic
        rate_from = rates[c1]["rate"]
        rate_to = rates[c2]["rate"]
        conversion_rate = rate_to / rate_from
        converted_amount = amount * conversion_rate

        return {
            "amount": amount,
            "from": c1,
            "to": c2,
            "result": round(converted_amount, 2),
            "rate": round(conversion_rate, 4),
            "recorded_date": recorded_at,
        }

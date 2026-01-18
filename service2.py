from decimal import Decimal
from sqlalchemy import text
from database import engine
from datetime import datetime, timezone


class ExchangeRateService:
    BASE_CURRENCY = "USD"

    def convert(self, from_currency: str, to_currency: str, amount: Decimal) -> dict:
        # 1. Use values exactly as provided (No .upper() or .strip())
        c1, c2 = from_currency, to_currency

        with engine.connect() as connection:
            query = text(
                """
                WITH all_prices AS (
                    SELECT currency_code AS code, rate, recorded_at AS ts, 'forex' AS source
                    FROM exchange_rates
                    WHERE currency_code IN (:c1, :c2)
                    UNION ALL
                    SELECT symbol AS code, current_price AS rate, last_updated::timestamp AS ts, 'crypto' AS source
                    FROM crypto_prices
                    WHERE symbol IN (:c1, :c2)
                )
                SELECT DISTINCT ON (code) code, rate, ts, source
                FROM all_prices
                ORDER BY code, ts DESC
            """
            )
            result = connection.execute(query, {"c1": c1, "c2": c2})

            # 2. Map results using raw row.code
            rates = {
                row.code: {
                    "rate": Decimal(str(row.rate)),
                    "timestamp": row.ts,
                    "source": row.source,
                }
                for row in result
            }

        # 3. Handle Base Currency (Still matching raw input)
        if c1 == self.BASE_CURRENCY:
            rates[c1] = {
                "rate": Decimal("1.0"),
                "source": "base",
                "timestamp": datetime.now(timezone.utc),
            }
        if c2 == self.BASE_CURRENCY:
            rates[c2] = {
                "rate": Decimal("1.0"),
                "source": "base",
                "timestamp": datetime.now(timezone.utc),
            }

        # 4. Validation
        if c1 not in rates or c2 not in rates:
            missing = [c for c in [c1, c2] if c not in rates]
            raise ValueError(f"Currencies not found: {', '.join(missing)}")

        # 5. The Math Logic (Value in Base)
        def get_value_in_base(code):
            data = rates[code]
            if data["source"] == "crypto" or data["source"] == "base":
                return data["rate"]  # USD per unit
            else:
                return Decimal("1.0") / data["rate"]  # 1 / (Units per USD)

        val_from = get_value_in_base(c1)
        val_to = get_value_in_base(c2)

        conversion_rate = val_from / val_to
        converted_amount = amount * conversion_rate

        return {
            "amount": float(amount),
            "from": c1,
            "to": c2,
            "result": round(float(converted_amount), 8),
            "rate": round(float(conversion_rate), 8),
            "last_updated": rates[c1].get("timestamp"),
        }

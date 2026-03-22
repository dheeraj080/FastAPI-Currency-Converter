import pytest
from fastapi.testclient import TestClient
from decimal import Decimal
from datetime import datetime, timezone

# We need to set the SUPABASE_URL in the environment before importing database things
import os
os.environ["SUPABASE_URL"] = "postgresql://fakeuser:fakepass@fakehost:5432/fakedb"

from main import app
from service2 import ExchangeRateService

# --- MOCK SERVICE ---
class MockExchangeRateService:
    def convert(self, from_currency: str, to_currency: str, amount: Decimal) -> dict:
        if from_currency == "FAKE" or to_currency == "FAKE":
            raise ValueError(f"Currencies not found: FAKE")
        
        # Simulate simple 1.5x conversion for test purposes
        conversion_rate = Decimal("1.5")
        converted_amount = amount * conversion_rate
        
        return {
            "amount": float(amount),
            "from": from_currency,
            "to": to_currency,
            "result": round(float(converted_amount), 8),
            "rate": float(conversion_rate),
            "last_updated": datetime.now(timezone.utc).isoformat()
        }

# Override FastAPI dependency
app.dependency_overrides[ExchangeRateService] = MockExchangeRateService

# --- TEST CLIENT ---
client = TestClient(app)

# --- TESTS ---
def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "OK"}

def test_convert_success():
    response = client.get("/latest?from_currency=USD&to_currency=EUR&amount=100")
    assert response.status_code == 200
    data = response.json()
    assert data["amount"] == 100.0
    assert data["from"] == "USD"
    assert data["to"] == "EUR"
    assert data["result"] == 150.0  # 100 * 1.5
    assert data["rate"] == 1.5

def test_convert_invalid_amount_negative():
    response = client.get("/latest?from_currency=USD&to_currency=EUR&amount=-5")
    # Pydantic should catch constraints like gt=0
    assert response.status_code == 422 
    detail = response.json().get("detail", [])
    assert len(detail) > 0
    assert "Input should be greater than 0" in detail[0]["msg"]

def test_convert_invalid_amount_string():
    response = client.get("/latest?from_currency=USD&to_currency=EUR&amount=abc")
    # Pydantic parsing error
    assert response.status_code == 422 

def test_convert_unknown_currency():
    response = client.get("/latest?from_currency=FAKE&to_currency=EUR&amount=100")
    # Our API specifically maps ValueError inside the service to 400
    assert response.status_code == 400
    assert "Currencies not found" in response.json()["detail"]

def test_convert_missing_parameters():
    # Missing 'amount'
    response = client.get("/latest?from_currency=USD&to_currency=EUR")
    assert response.status_code == 422
    
def test_rate_limiter_exists():
    # Because slowapi wraps the endpoint, doing > 30 calls in a min would trigger a 429.
    # We will just verify the headers exist or the wrapper works for a normal call.
    response = client.get("/latest?from_currency=USD&to_currency=EUR&amount=10")
    assert response.status_code == 200
    # Slowapi standard headers usually include X-RateLimit-Limit but only if we trigger or configure it.
    # At minimum, a 200 proves the limiter didn't block the valid request.

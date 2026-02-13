# Currency Exchange API

A robust **FastAPI** service designed for real-time and historical currency conversions. It uniquely handles a hybrid data model, allowing seamless conversion between traditional **Forex** (fiat) and **Cryptocurrency** assets by normalizing values against a USD base.

---

## 🚀 Key Features

* **Dual-Source Engine**: Integrates both `exchange_rates` (Forex) and `crypto_prices` (Crypto) tables to calculate cross-rates.
* **Precision Math**: Uses the `Decimal` type to ensure financial accuracy and avoid floating-point errors during conversion.
* **Rate Limiting**: Built-in protection via `slowapi` to prevent service abuse, configured at 30 requests per minute.
* **Historical Support**: Capability to query rates based on specific timestamps with UTC normalization (endpoint currently in review).
* **Database Resilience**: Uses SQLAlchemy with `pool_pre_ping=True` to maintain stable connections to PostgreSQL/Supabase.

---

## 🛠️ Architecture

The application is structured into modular components:
* **`api.py`**: Defines REST endpoints, Pydantic query validation, and rate limiting logic.
* **`service2.py`**: Contains the core business logic for calculating values in a USD base currency for both fiat and crypto.
* **`models.py`**: Defines the SQLAlchemy ORM structures for conversions and rate tracking.
* **`database.py`**: Manages the SQLAlchemy engine connection and environment variable loading.

---

## 🔧 Installation & Setup

### 1. Prerequisites
* Python 3.10+
* A PostgreSQL database (e.g., Supabase) with `exchange_rates` and `crypto_prices` tables.

### 2. Environment Configuration
Create a `.env` file in the root directory and add your connection string:
```env
SUPABASE_URL=postgresql://user:password@host:port/dbname

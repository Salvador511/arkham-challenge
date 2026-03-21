# Arkham Challenge - Nuclear Outages Monitoring API

A comprehensive system for extracting, storing, and serving U.S. nuclear power plant outage data from the Energy Information Administration (EIA) API.

## 📋 Overview

Technical challenge project: A REST API that extracts, stores, and serves nuclear power plant outage data from the EIA. Query facility-level or national outage statistics with flexible filtering and automated data updates.

## 🏗️ Project Structure

```
arkham-challenge/
├── backend/                    # Python FastAPI application
│   ├── app/
│   │   ├── main.py            # FastAPI application entry point
│   │   ├── config.py          # Application configuration
│   │   ├── error_handlers.py  # Global error handling
│   │   ├── exceptions.py      # Custom exception definitions
│   │   ├── routes/
│   │   │   ├── data.py        # GET /data endpoint (query outages)
│   │   │   └── refresh.py     # POST /refresh endpoint (trigger extraction)
│   │   └── schemas/
│   │       └── responses.py   # Pydantic response models
│   ├── connector/
│   │   ├── config.py          # EIA API configuration
│   │   ├── extract_data.py    # Data extraction logic
│   │   └── state_manager.py   # Extraction state tracking
│   ├── services/
│   │   ├── data_service.py    # Data querying & filtering
│   │   └── refresh_service.py # Extraction orchestration
│   ├── storage/               # Delta Lake data store
│   ├── requirements.txt       # Python dependencies
│   ├── pytest.ini            # Test configuration
│   └── ruff.toml             # Code style configuration
├── frontend/                  # Frontend application
├── diagrams/                  # Architecture documentation
└── README.md                  # This file
```

## 🚀 Features

### Data Management
- **Automated Extraction**: Pulls real-time nuclear outage data from EIA API
- **Full & Incremental Modes**: Initial full load followed by efficient incremental updates
- **Concurrent Protection**: Lock mechanism prevents overlapping data extractions
- **Retry Logic**: Automatic retry with exponential backoff for network failures

### REST API
- **`GET /data`** - Query nuclear outages with flexible filtering:
  - Filter by dataset: `facility` or `us` (aggregated)
  - Date range filtering (YYYY-MM-DD format)
  - Facility-specific queries
  - Pagination support (offset/limit, max 1000 records)

- **`POST /refresh`** - Manually trigger data extraction
  - Returns `202 Accepted` for async extraction in progress
  - Returns `200 OK` with data when extraction completes immediately

- **`GET /health`** - API health check

### Historical Data Storage
- **Delta Lake**: Versioned, ACID-compliant data lake on Parquet files
- **Transaction Logs**: Full audit trail of all data changes
- **Point-in-Time Queries**: Access historical snapshots of data
- **Efficient Compression**: Parquet columnar format for optimal storage

### Data Models

**Facility Outages** - Per-plant outage tracking:
```json
{
  "facility_id": "152",
  "facility_name": "Peach Bottom Atomic Power Station",
  "date": "2024-03-20",
  "capacity": 2350000,
  "outage": 1175000,
  "percent_outage": 50.0
}
```

**US Outages** - National aggregate:
```json
{
  "date": "2024-03-20",
  "capacity": 112000000,
  "outage": 8000000,
  "percent_outage": 7.14
}
```

## 💻 Backend Setup

### Prerequisites
- Python 3.9+
- EIA API Key (get from https://www.eia.gov/opendata/)

### Installation

```bash
cd backend
pip install -r requirements.txt
```

### Configuration

Set your EIA API key:

```bash
export EIA_API_KEY="your-api-key-here"
```

Or create a `.env` file in the `backend/` directory:

```env
EIA_API_KEY=your-api-key-here
```

App settings can be modified in [backend/app/config.py](backend/app/config.py):
- Default pagination limit: 100 records
- Maximum pagination limit: 1000 records
- Supported datasets: `facility`, `us`

Connector settings (API timeouts, retries) in [backend/connector/config.py](backend/connector/config.py):
- API timeout: 30 seconds
- Max retries: 2 attempts
- EIA page size: 5,000 records per request

### Running the Server

```bash
cd backend
uvicorn app.main:app --reload
```

API will be available at `http://localhost:8000`

Interactive API docs: `http://localhost:8000/docs`

### Running Tests

```bash
cd backend
pytest
```

With coverage report:

```bash
pytest --cov=app --cov=connector --cov=services
```

## 📊 API Examples

### Query facility outages
```bash
curl "http://localhost:8000/data?dataset=facility&facility_id=152&date_after=2024-03-01"
```

### Query US aggregate outages with pagination
```bash
curl "http://localhost:8000/data?dataset=us&date_after=2024-01-01&offset=0&limit=100"
```

### Trigger data refresh
```bash
curl -X POST "http://localhost:8000/refresh"
```

## 🛠️ Technology Stack

| Component      | Technology |
|---|---|
| **Framework** | FastAPI + Uvicorn |
| **Data Processing** | Pandas + PyArrow |
| **Data Lake** | Delta Lake v1.5+ (Parquet) |
| **Config Management** | Pydantic + Pydantic Settings |
| **Testing** | pytest + pytest-asyncio |
| **Code Quality** | ruff |
| **Data Source** | EIA API (`https://api.eia.gov/v2/nuclear-outages/`) |

## 📦 Dependencies

See [backend/requirements.txt](backend/requirements.txt) for full dependency list.

Key libraries:
- `fastapi` - Web framework
- `uvicorn` - ASGI server
- `pandas` - Data manipulation
- `pyarrow` - Arrow data format
- `deltalake` - Delta Lake support
- `requests` - HTTP client
- `pydantic` - Data validation
- `python-dotenv` - Environment variables

## 🔄 Data Flow

1. **Server Startup**: Automatically checks and performs data extraction
   - First run: Full extraction from EIA API
   - Subsequent runs: Incremental extraction (only new/changed records)

2. **User Query**: Client calls `/data` endpoint with filters
   - DataService loads Parquet files from storage
   - Applies date, facility, pagination filters
   - Returns paginated JSON response

3. **Manual Refresh**: Client calls `/POST /refresh` to force extraction
   - Lock mechanism ensures only one extraction at a time
   - Returns 202 if extraction is in progress
   - Returns 200 when extraction completes

4. **State Tracking**: `storage/delta/state.json` stores:
   - Last extraction timestamp
   - Incremental cursor position
   - Extraction status and metadata

## ⚠️ Error Handling

The API provides standardized error responses with appropriate HTTP status codes:

- **400 Bad Request** - Invalid query parameters or validation errors
- **404 Not Found** - Requested data does not exist
- **409 Conflict** - Another extraction is already in progress
- **500 Internal Server Error** - Unexpected API or processing errors

All error responses include:
- Error message with details
- Error code for programmatic handling
- Retry guidance for transient failures

## 🧪 Testing

Tests are located throughout the codebase:
- [backend/app/test_exceptions.py](backend/app/test_exceptions.py) - Exception handling tests
- [backend/connector/test_extract_data.py](backend/connector/test_extract_data.py) - Data extraction tests
- [backend/connector/test_state_manager.py](backend/connector/test_state_manager.py) - State management tests
- [backend/services/test_data_service.py](backend/services/test_data_service.py) - Query service tests

Run tests with:
```bash
pytest -v                    # Verbose output
pytest --lf                  # Run last failed tests
pytest -k "test_name"        # Run specific test
```

## 📝 Notes

- Data extraction requires valid EIA API credentials
- First extraction may take several minutes (full dataset load)
- Delta Lake maintains transaction logs for data integrity
- API is designed for read-heavy production workloads
- Incremental extraction optimizes for minimal data transfer

## 📧 Support

For issues or questions, refer to the error logs and API documentation at `/docs`

---

**Status**: Active Development  
**Last Updated**: March 20, 2026  
**Author**: Salvador Orozco

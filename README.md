# Arkham Challenge - Nuclear Outages Data Pipeline

Data pipeline and dashboard that extracts Nuclear Outages data from the EIA Open Data API, stores it in Parquet/Delta, exposes it through a REST API, and visualizes it in a web interface.

## Quick Start

### 1) Prerequisites

- Python 3.12+
- Node.js 20+
- npm 10+
- EIA API key from https://www.eia.gov/opendata/

### 2) Backend setup

```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3) API key setup

Option A (environment variable):

```bash
export EIA_API_KEY="your-api-key-here"
```

Option B (`backend/.env` file):

```env
EIA_API_KEY=your-api-key-here
```

Optional (cloud/local DB storage):

```env
DATABASE_URL=postgresql://user:password@host:5432/database
```

### 4) Run backend API

```bash
cd backend
source venv/bin/activate
uvicorn app.main:app --reload
```

API base URL: `http://localhost:8000`  
Docs: `http://localhost:8000/docs`

### 5) Run frontend

```bash
cd frontend
npm install
```

Create `frontend/.env`:

```env
VITE_API_BASE_URL=http://localhost:8000/
```

Run app:

```bash
npm run dev
```

Frontend URL: `http://localhost:5173`

### 6) Verify it is running

- Open frontend: `http://localhost:5173`
- Open backend docs: `http://localhost:8000/docs`

## API Key and Extraction Behavior

- On startup, backend runs extraction automatically.
- First run performs **full extraction**.
- Next runs perform **incremental extraction** using saved state.
- Missing or invalid key returns a clear connector error.

## Core Endpoints

- `GET /data`: query datasets with filters and pagination
  - `dataset`: `facility` | `us` | `plants`
  - `date_from`, `date_to` (YYYY-MM-DD)
  - `facility_id` (facility only)
  - `offset`, `limit`
- `POST /refresh`: trigger full/incremental extraction in background
- `GET /refresh/status`: poll extraction status
- `GET /health`: service health check

## Result Examples

### Example 1: Query facility outages

```bash
curl "http://localhost:8000/data?dataset=facility&facility_id=152&date_from=2024-03-01&limit=2"
```

Example response:

```json
{
  "status": "success",
  "total_count": 24,
  "offset": 0,
  "limit": 2,
  "returned": 2,
  "data": [
    {
      "date": "2024-03-20",
      "facility_id": "152",
      "facility_name": "Peach Bottom Atomic Power Station",
      "capacity": 2350000,
      "outage": 1175000,
      "percent_outage": 50.0
    }
  ]
}
```

### Example 2: Trigger refresh

```bash
curl -X POST "http://localhost:8000/refresh"
```

Example response:

```json
{
  "status": "processing",
  "extraction_type": "incremental",
  "message": "Incremental extraction started. Please check back shortly.",
  "retry_after_seconds": 60
}
```

### Example 3: Poll refresh status

```bash
curl "http://localhost:8000/refresh/status"
```

Example response:

```json
{
  "status": "idle",
  "message": "No extraction in progress"
}
```

## Assumptions Made

1. EIA outage API remains available and stable for required fields (`period`, `capacity`, `outage`, `percentOutage`, and facility fields).
2. Date values are handled as `YYYY-MM-DD` across API filters and stored records.
3. Single backend instance is expected in local mode; lock file is used to avoid concurrent extraction.
4. Local filesystem storage is valid for development; `DATABASE_URL` enables persistent cloud-friendly storage.
5. Frontend consumes backend directly through `VITE_API_BASE_URL`.

## Technical Decisions (Brief)

- **FastAPI** for a minimal REST API with automatic OpenAPI documentation.
- **Pandas + Delta/Parquet** for efficient columnar storage and simple local operation.
- **Incremental extraction with persisted state** to avoid reprocessing full history.
- **Background refresh + status polling** to keep API responses fast during long extractions.
- **React + TanStack Query** for frontend data fetching, caching, and refresh UX.

## Data Model

- `plants` (`facility_id` PK, `facility_name`)
- `facility_outages` (`date`, `facility_id`, `capacity`, `outage`, `percent_outage`)
- `us_outages` (`date`, `capacity`, `outage`, `percent_outage`)

ER diagram: [diagrams/ER diagram.pdf](diagrams/ER%20diagram.pdf)

## Quality Checks

Backend tests:

```bash
cd backend
source venv/bin/activate
pytest
```

Coverage:

```bash
pytest --cov=app --cov=connector --cov=services
```

Frontend quality checks:

```bash
cd frontend
npm run lint
npm run build
```

## Project Structure

```
arkham-challenge/
├── backend/
│   ├── app/
│   ├── connector/
│   ├── services/
│   └── storage/
├── frontend/
├── diagrams/
└── README.md
```

---

**Author**: Salvador Orozco

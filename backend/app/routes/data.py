"""Data endpoints - GET /data"""

from fastapi import APIRouter, Query

from app.core.exceptions import ValidationError
from services.data_service import DataService

router = APIRouter(tags=["Data"])


@router.get("/data", summary="Get outages and plants data")
async def get_data(
    dataset: str | None = Query(
        None,
        description="Dataset to fetch: 'facility', 'us', or 'plants'. If not specified, returns all.",
    ),
    date_from: str | None = Query(None, description="Filter from date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="Filter to date (YYYY-MM-DD)"),
    facility_id: str | None = Query(
        None, description="Filter by facility_id (only for 'facility' dataset)"
    ),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Pagination limit"),
):
    """
    Fetch nuclear outages data with filtering and pagination.

    - **dataset**: 'facility', 'us', or 'plants' (optional, returns all if not specified)
    - **date_from**: Start date in YYYY-MM-DD format
    - **date_to**: End date in YYYY-MM-DD format
    - **facility_id**: Filter by facility (only for 'facility' dataset)
    - **offset**: Pagination offset (default: 0)
    - **limit**: Pagination limit (default: 100, max: 1000)
    """
    if facility_id is not None:
        if dataset is not None and dataset != "facility":
            raise ValidationError("facility_id parameter only allowed for 'facility' dataset")
        dataset = "facility"

    if dataset is None:
        facility_data = DataService.get_dataset(
            dataset="facility",
            date_from=date_from,
            date_to=date_to,
            offset=offset,
            limit=limit,
        )
        us_data = DataService.get_dataset(
            dataset="us",
            date_from=date_from,
            date_to=date_to,
            offset=offset,
            limit=limit,
        )
        plants_data = DataService.get_dataset(
            dataset="plants",
            offset=offset,
            limit=limit,
        )

        return {
            "status": "success",
            "facility_outages": {
                "status": "success",
                **facility_data,
            },
            "us_outages": {
                "status": "success",
                **us_data,
            },
            "plants": {
                "status": "success",
                **plants_data,
            },
        }

    data = DataService.get_dataset(
        dataset=dataset,
        date_from=date_from,
        date_to=date_to,
        facility_id=facility_id,
        offset=offset,
        limit=limit,
    )

    return {
        "status": "success",
        **data,
    }

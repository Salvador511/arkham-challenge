from dotenv import load_dotenv

# Load environment variables BEFORE importing settings
load_dotenv()

import logging  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402

from app.config import settings  # noqa: E402
from app.core.logging import configure_logging  # noqa: E402
from app.error_handlers import register_error_handlers  # noqa: E402
from app.routes import data, refresh  # noqa: E402
from services.refresh_service import run_extraction  # noqa: E402

logger = logging.getLogger(__name__)

configure_logging()


async def startup_extraction():
    """
    Execute data extraction when server starts.

    - First startup: Runs full extraction
    - Subsequent startups: Runs incremental extraction
    """
    logger.info("=" * 70)
    logger.info("🚀 Server starting - Running startup extraction...")
    logger.info("=" * 70)

    try:
        result = run_extraction()
        logger.info("✅ Startup extraction successful")
        logger.info(f"Status: {result['status']}")
    except Exception as exc:
        logger.error(f"❌ Startup extraction failed: {exc}")
        logger.warning("Server is starting but data extraction failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage FastAPI application lifecycle.

    - Startup: Run data extraction
    - Shutdown: Cleanup (if needed)
    """
    await startup_extraction()
    yield
    logger.info("Server shutting down...")


app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    debug=settings.debug,
    docs_url=settings.docs_url,
    redoc_url=settings.redoc_url,
    openapi_url=settings.openapi_url,
    lifespan=lifespan,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_error_handlers(app)

app.include_router(data.router)
app.include_router(refresh.router)


@app.get("/health")
async def health_check():
    """Check API health status."""
    return {"status": "healthy", "version": settings.app_version}

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.error_handlers import register_error_handlers
from app.routes import data, refresh
from services.refresh_service import run_extraction

logger = logging.getLogger(__name__)


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
        # Log error but don't crash the server
        logger.error(f"❌ Startup extraction failed: {exc}")
        logger.warning("Server is starting but data extraction failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage FastAPI application lifecycle.

    - Startup: Run data extraction
    - Shutdown: Cleanup (if needed)
    """
    # Startup
    await startup_extraction()
    yield
    # Shutdown
    logger.info("Server shutting down...")


app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    debug=settings.debug,
    lifespan=lifespan,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_error_handlers(app)

app.include_router(data.router)
app.include_router(refresh.router)


@app.get("/health")
async def health_check():
    """Verifica que la API está funcionando"""
    return {"status": "healthy", "version": settings.app_version}

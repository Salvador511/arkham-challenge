from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.error_handlers import register_error_handlers
from app.routes import data

app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    debug=settings.debug,
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

@app.get("/health")
async def health_check():
    """Verifica que la API está funcionando"""
    return {"status": "healthy", "version": settings.app_version}

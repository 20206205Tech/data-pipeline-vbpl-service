from fastapi import APIRouter

from app.documents.router import router as documents_router
from app.workflows.router import router as workflows_router

index_router = APIRouter()

index_router.include_router(workflows_router)
index_router.include_router(documents_router)

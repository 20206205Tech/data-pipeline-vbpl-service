from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from lifespan.shutdown import shutdown
from lifespan.startup import startup


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server is starting...")
    await startup(app)

    yield

    logger.info("Server is shutting down...")
    await shutdown(app)

from fastapi import FastAPI
from loguru import logger


async def shutdown(app: FastAPI):
    logger.info("Shutting down application processes...")

    # if hasattr(app.state, "redis"):
    #     await app.state.redis.close()
    #     logger.info("Redis connection closed")

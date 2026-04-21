from fastapi import FastAPI

# from fastapi_cache import FastAPICache
# from fastapi_cache.backends.redis import RedisBackend
from loguru import logger

# from redis import asyncio as aioredis

# import env


async def startup(app: FastAPI):
    logger.info("Starting application processes...")

    # redis = aioredis.from_url(env.REDIS_URL, encoding="utf8", decode_responses=True)
    # FastAPICache.init(RedisBackend(redis), prefix=env.CACHE_KEY)

    # app.state.redis = redis

    # logger.info(f"Redis cache initialized with prefix: {env.CACHE_KEY}")

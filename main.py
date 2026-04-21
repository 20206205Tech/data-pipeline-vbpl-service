from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_voyager import create_voyager

import env
from index_router import index_router
from lifespan.lifespan import lifespan
from middlewares.log_request_and_response_middleware import (
    LogRequestAndResponseMiddleware,
)
from utils.log_function import log_function

app = FastAPI(
    lifespan=lifespan,
    title=f"{env.SERVICE_NAME} ({env.ENVIRONMENT})",
    description=env.DESCRIPTION,
    swagger_ui_parameters={"persistAuthorization": True},
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(LogRequestAndResponseMiddleware)


app.include_router(index_router, prefix="/api")


@app.get("/")
@log_function
def root():
    return {"message": "Hello World", "docs": "/docs"}


app.mount(
    "/voyager",
    create_voyager(
        app,
        module_color={"common": "blue", "app": "red", "app": "green"},
        swagger_url="/docs",
    ),
)


def main():
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=env.PORT, reload=env.RELOAD)


if __name__ == "__main__":
    main()

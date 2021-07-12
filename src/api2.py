from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import motor.motor_asyncio
import os
import pytz
import bson

from pydantic.main import BaseModel

from framework.src.chain_factory.task_queue.wrapper.redis_client import RedisClient

from login_api.login_api2 import (
    init_app,
    app as login_app,
    route_authorization,
)

from orchestrator_api.api import app as orchestrator_app

app = FastAPI()

origins = [
    "http://backend-api.ad.lan",
    "https://backend-api.ad.lan",
    "http://web-ui.ad.lan",
    "https://web-ui.ad.lan",
    "http://chainfactory.ad.lan",
    "https://chainfactory.ad.lan",
    "http://localhost",
    "http://localhost:5002",
    "http://localhost:5003",
    "http://localhost:5004",
    "http://localhost:3000",
    "http://localhost:4200",
    "https://localhost:5002",
    "https://localhost:5003",
    "https://localhost:5004",
    "https://localhost:3000",
    "https://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

host = "127.0.0.1"
rabbitmq_host = os.getenv("RABBITMQ_HOST", host)
rabbitmq_user = os.getenv("RABBITMQ_USER", "guest")
rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "guest")


MONGO_DETAILS = "mongodb://root:example@" + host + "/orchestrator_db?authSource=admin"
REDIS_HOST = os.getenv("REDIS_HOST", host)

client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)

database = client.orchestrator_db

init_app(database)

secret_key = "asecret128bitkeyasecret128bitkey"

redis_client: RedisClient = RedisClient(REDIS_HOST)

app.include_router(login_app, prefix='/api/login')
app.include_router(orchestrator_app, prefix='/api/orchestrator')
app.middleware("http")(route_authorization)


@app.middleware("http")
async def set_request_parameter(request: Request, call_next):
    request.state.secret_key = secret_key
    request.state.database = database
    request.state.redis_client = redis_client
    request.state.rabbitmq_host = rabbitmq_host
    request.state.rabbitmq_user = rabbitmq_user
    request.state.rabbitmq_password = rabbitmq_password
    return await call_next(request)


@app.get("/")
def read_root():
    return {"Hello": "World"}


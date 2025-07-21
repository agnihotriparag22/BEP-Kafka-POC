from fastapi import FastAPI
from app.api.v1.endpoints.validate import router as validate_router

app = FastAPI()
app.include_router(validate_router) 
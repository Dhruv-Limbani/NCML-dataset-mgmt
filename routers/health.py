# fastapi
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

# db
from db_service.mongodb_service import conn

# models and schemas
from schemas.dataset import datasetsEntity

# utilities
from utils.utils import check_db_connection

health_router = APIRouter()

@health_router.get("/", tags=["health"], response_model=dict,
    responses={
        200: {"description": "Database is live"},
        500: {"description": "Database not live"}
    },
   description="Health check for the database."
)
async def health_check():
    """Checks database connection and returns a list of datasets if the DB is live."""
    check_db_connection(conn)
    datasets = list(conn.local.datasets.find())
    datasets = datasetsEntity(datasets)
    if len(datasets) != 0:
        return JSONResponse(status_code=200, content={"datasets": datasets})
    raise HTTPException(status_code=200, detail={"message": "Database is empty"})
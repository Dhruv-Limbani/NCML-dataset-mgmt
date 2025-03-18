# fastapi
from fastapi import APIRouter, HTTPException, status, UploadFile
from fastapi.responses import JSONResponse, FileResponse

# db
from pymongo import ReturnDocument
from db_service.mongodb_service import conn

# models and schemas
from models.dataset import Dataset
from schemas.dataset import datasetEntity, datasetsEntity

# utilities
from utils.utils import check_db_connection, update_files, write_file, delete_file

# os and environment
import os
from dotenv import load_dotenv
load_dotenv()

STORAGE_LOC = os.getenv("STORAGE_LOC")

dataset_router = APIRouter()

@dataset_router.get('/all', tags=['dataset'],
    responses={
        200: {"description": "Datasets retrieved successfully"},
        204: {"description": "No datasets found"},
        404: {"description": "User/Project not found"},
        500: {"description": "Internal server error"},
    },
    description="This endpoint retrieves all the datasets of the given user for a particular project."
)
async def get_datasets(email: str, pname: str):

    check_db_connection(conn)

    if conn.local.projects.find_one({"email": email, "name": pname}) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User/Project not found")

    datasets = conn.local.datasets.find({"email": email, "pname": pname})

    if datasets is not None:
        datasets = datasetsEntity(datasets)
        if len(datasets) != 0:
            return JSONResponse(status_code=status.HTTP_200_OK, content={"datasets": datasets})

        raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)

@dataset_router.get('/', tags=['dataset'],
    responses={
        200: {"description": "Dataset retrieved successfully"},
        404: {"description": "Dataset not found"},
        500: {"description": "Internal server error"}
    },
    description="This endpoint retrieves a dataset with the given details."
)
async def get_dataset(email: str, pname: str, name: str):

    check_db_connection(conn)

    exists =  conn.local.datasets.find_one({"email": email, "pname": pname, "name": name})

    if exists is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    path = STORAGE_LOC + os.path.join(email, pname, 'datasets', name)

    return FileResponse(status_code=status.HTTP_200_OK, path=path, filename=name, media_type="text/csv")

@dataset_router.post("/", tags=["dataset"],
    responses = {
        200: {"description": "Dataset saved successfully"},
        400: {"description": "Bad request"},
        404: {"description": "User/Project not found"},
        409: {"description": "Name already in use"},
        500: {"description": "Internal server error"}
    },
    description = "This endpoint saves a dataset with the given details."
)
async def create_dataset(email: str, pname: str, file: UploadFile):

    if file.content_type != "text/csv":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type")

    check_db_connection(conn)

    try:

        if conn.local.projects.find_one({"email": email, "name": pname}) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User/Project not found")

        if conn.local.datasets.find_one({"email": email, "pname": pname, "name":file.filename}):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"A dataset with name: {file.filename} already exists under this email and project!")

        path = STORAGE_LOC + os.path.join(email, pname, 'datasets', file.filename)

        write_file(path, file)

        dataset = {
            "email": email,
            "pname": pname,
            "name" : file.filename
        }

        conn.local.datasets.insert_one(dataset)

        return JSONResponse(status_code=status.HTTP_200_OK, content={"dataset":datasetEntity(dataset)})

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        path = STORAGE_LOC + os.path.join(email, pname, 'datasets', file.filename)
        delete_file(path)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@dataset_router.put("/", tags=["dataset"],
    responses={
        200: {"description": "Dataset updated successfully"},
        400: {"description": "Bad request"},
        404: {"description": "Project/Dataset not found"},
        409: {"description": "Name already in use"},
        500: {"description": "Internal server error"}
    },
    description="This endpoint updates the dataset of an existing project."
)
async def update_dataset(email: str, pname: str, name: str, file: UploadFile):

    if file.content_type != "text/csv":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type")

    check_db_connection(conn)

    if conn.local.datasets.find_one({"email": email, "pname": pname, "name": name}) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project/Dataset not found")

    if name != file.filename and conn.local.datasets.find_one({"email": email, "pname": pname, "name":file.filename}) is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"A dataset with name: {file.filename} already exists under this email and project!")

    try:

        update_files(email, pname, name, file)
        update_data = {
            "email": email,
            "pname": pname,
            "name": file.filename
        }
        updated_dataset = conn.local.datasets.find_one_and_update(
            {"email": email, "pname": pname, "name":name},
            {'$set': update_data},
            return_document=ReturnDocument.AFTER
        )

        if not updated_dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        return JSONResponse(status_code=status.HTTP_200_OK, content={"dataset": datasetEntity(updated_dataset)})

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@dataset_router.delete("/", tags=["dataset"],
    responses={
        200: {"description": "Dataset deleted successfully"},
        404: {"description": "Dataset not found"},
        500: {"description": "Internal server error"}
    },
    description="This endpoint deletes a dataset by it's user, project name and name"
)
async def delete_dataset(dataset: Dataset):

    check_db_connection(conn)

    deleted_dataset = conn.local.datasets.find_one_and_delete({"email": dataset.email, "pname": dataset.pname, "name": dataset.name})

    if not deleted_dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    file_loc = STORAGE_LOC + os.path.join(dataset.email, dataset.pname, 'datasets', dataset.name)

    delete_file(file_loc)

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Dataset deleted successfully!"})
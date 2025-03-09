# fastapi
from fastapi import APIRouter, HTTPException, status, UploadFile
from fastapi.responses import JSONResponse

# db
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from db_service.mongodb_service import conn

# models and schemas
from models.dataset import Dataset
from schemas.dataset import datasetEntity, datasetsEntity

# utilities
from utils.utils import check_db_connection, is_authorized, save_file, update_files, delete_files

dataset_router = APIRouter()

@dataset_router.get('/all', tags=['dataset'],
    responses={
        200: {"description": "Datasets Retrieved Successfully!"},
        400: {"description": "Bad Request!"},
        500: {"description": "Database Not Live!"}
    },
    description="This endpoint retrieves all the datasets of the given user for a particular project."
)
async def get_datasets(email: str, pname: str):

    check_db_connection(conn)

    datasets = conn.local.datasets.find({"email": email, "pname": pname})

    if datasets is not None:
        datasets = datasetsEntity(datasets)
        if len(datasets) != 0:
            return JSONResponse(status_code=200, content={"datasets":datasets})

    raise HTTPException(status_code=400, detail="Datasets not found!")

@dataset_router.get('/', tags=['dataset'],
    responses={
        200: {"description": "Dataset Retrieval Successful!"},
        400: {"description": "Bad Request!"},
        500: {"description": "Database Not Live!"}
    },
    description="This endpoint retrieves a dataset with the given details."
)
async def get_dataset(email: str, pname: str, name: str):

    check_db_connection(conn)

    dataset =  conn.local.datasets.find_one({"email": email, "name": name, "pname": pname})

    if dataset is None:
        raise HTTPException(status_code=400, detail="Dataset not found!")

    return JSONResponse(status_code=200, content={"dataset":datasetEntity(dataset)})

@dataset_router.post("/", tags=["dataset"],
    responses = {
        200: {"description": "Dataset Saved Successful!"},
        400: {"description": "Bad Request!"},
        500: {"description": "Database Not Live!"}
    },
    description = "This endpoint saves a dataset with the given details."
)
async def create_dataset(email: str, pname: str, file: UploadFile):

    if file.content_type != "text/csv":
        raise HTTPException(status_code=400, detail="Invalid file type!")

    check_db_connection(conn)
    try:
        if conn.local.datasets.find_one({"email": email, "name": file.filename, "pname": pname}):
            raise HTTPException(status_code=400, detail="Dataset with this name already exists!")

        dataset = save_file(email, pname, file)

        conn.local.datasets.insert_one(dict(dataset))
        return JSONResponse(status_code=200, content={"message": "Dataset saved successfully!", "dataset":datasetEntity(dataset)})

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@dataset_router.put("/", tags=["dataset"],
    responses={
        200: {"description": "Dataset modification successful!"},
        400: {"description": "Bad Request!"},
        404: {"description": "Dataset not found!"},
        409: {"description": "Name already in use!"},
        500: {"description": "Database Not Live!"}
    },
    description="This endpoint modifies the dataset of an existing project."
)
async def update_dataset(email: str, pname: str, name: str, file: UploadFile):

    check_db_connection(conn)

    if conn.local.datasets.find_one({"email": email, "pname": pname, "name":name}) is None:
        raise HTTPException(status_code=400, detail="Dataset does not exists!")

    if name != file.filename and conn.local.datasets.find_one({"email": email, "pname": pname, "name":file.filename}) is not None:
        raise HTTPException(status_code=400, detail=f"A project with name: {file.filename} already exists under this email!")

    try:
        new = update_files(email, pname, name, file)
        update_data = {k: v for k, v in dict(new).items() if k not in ["_id", "email", "pname"]}
        updated_dataset = conn.local.datasets.find_one_and_update(
            {"email": email, "pname": pname, "name":name},
            {'$set': update_data},
            return_document=ReturnDocument.AFTER  # Return the updated user
        )
        print(1)
        if not updated_dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        print(datasetEntity(updated_dataset)["content"])
        return JSONResponse(status_code=200, content={"message": "Project updated successfully", "dataset": datasetEntity(updated_dataset)})

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@dataset_router.delete("/", tags=["dataset"],
    responses={
        200: {"description": "Dataset deletion successful!"},
        404: {"description": "Dataset not found!"},
        500: {"description": "Database Not Live!"}
    },
    description="This endpoint deletes a dataset by it's user, project name and name"
)
async def delete_dataset(email: str, pname: str, name: str):

    check_db_connection(conn)

    deleted_dataset = conn.local.datasets.find_one_and_delete({"email": email, "pname": pname, "name": name})

    print(deleted_dataset)
    if not deleted_dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    delete_files(email, pname, name)

    return JSONResponse(status_code=200, content={"message": "Dataset deleted successfully!"})
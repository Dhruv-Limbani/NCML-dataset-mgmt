# fastapi
from fastapi import HTTPException, status, UploadFile

# models and schemas
from models.dataset import Dataset

# os and environment
import os, shutil
from dotenv import load_dotenv
load_dotenv()

# pandas
import pandas as pd

STORAGE_LOC = os.getenv("STORAGE_LOC")

def check_db_connection(conn):
    try:
        conn.server_info()
    except:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database not live")

def is_authorized(dataset: Dataset, email: str):
    if dataset.email != email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

def get_file(loc):
    path = STORAGE_LOC + loc
    with open(path, 'r') as file:
        file_content = file.read()
    return file_content

def write_file(loc, file):
    path = STORAGE_LOC + loc
    with open(path, "wb") as buffer:
        buffer.write(file.file.read())
    return

def save_file(email, pname, file):
    base_dir = os.path.join(STORAGE_LOC, email, pname, 'datasets')
    if not os.path.exists(base_dir):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    dataset_loc = os.path.join(email, pname, 'datasets', f"{file.filename}")

    write_file(dataset_loc, file)

    return Dataset(
        name = file.filename,
        email = email,
        pname = pname,
        content = dataset_loc
    )


def update_files(email, pname, name, file):

    base_dir = os.path.join(STORAGE_LOC, email, pname, 'datasets')
    if not os.path.exists(base_dir):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    if name != file.filename:
        new_name_path = os.path.join(base_dir, f"{file.filename}")
        old_name_path = os.path.join(base_dir, f"{name}")
        os.rename(old_name_path, new_name_path)

    dataset_loc = os.path.join(email, pname, 'datasets', f"{file.filename}")

    write_file(dataset_loc, file)

    return Dataset(
        name=file.filename,
        email=email,
        pname=pname,
        content=dataset_loc
    )

def delete_files(email, pname, name):
    path = os.path.join(STORAGE_LOC, email, pname, 'datasets', name)
    os.remove(path)
    return



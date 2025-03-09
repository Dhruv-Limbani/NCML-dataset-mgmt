# fastapi
from fastapi import HTTPException, status

# models and schemas
from models.dataset import Dataset

# os and environment
import os
from dotenv import load_dotenv
load_dotenv()

STORAGE_LOC = os.getenv("STORAGE_LOC")

def check_db_connection(conn):
    try:
        conn.server_info()
    except:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database not live")

def get_file_content(loc):
    path = STORAGE_LOC + loc
    with open(path, 'r') as file:
        file_content = file.read()
    return file_content

def write_file(loc, file):
    path = STORAGE_LOC + loc
    with open(path, "wb") as buffer:
        buffer.write(file.file.read())

def save_file(email, pname, file):

    dataset_loc = os.path.join(email, pname, 'datasets', f"{file.filename}")

    write_file(dataset_loc, file)

    return Dataset(
        email = email,
        pname = pname,
        name = file.filename,
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
        email = email,
        pname = pname,
        name=file.filename,
        content = dataset_loc
    )

def delete_files(email, pname, name):
    path = os.path.join(STORAGE_LOC, email, pname, 'datasets', name)
    os.remove(path)
    return
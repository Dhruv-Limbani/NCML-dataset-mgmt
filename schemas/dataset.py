from utils.utils import get_file_content

def datasetEntity(item) -> dict:
    return {
        "name":item["name"],
        "email":item["email"],
        "pname":item["pname"],
        "content":get_file_content(item["content"]),
    }

def datasetsEntity(items) -> list:
    return [datasetEntity(item) for item in items]
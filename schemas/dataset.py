def datasetEntity(item) -> dict:
    return {
        "name":item["name"],
        "email":item["email"],
        "pname":item["pname"],
    }

def datasetsEntity(items) -> list:
    return [datasetEntity(item) for item in items]
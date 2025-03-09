from pydantic import BaseModel

class Dataset(BaseModel):
    email: str
    pname: str
    name: str
    content: str
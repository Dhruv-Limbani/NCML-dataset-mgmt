from pydantic import BaseModel

class Dataset(BaseModel):
    name: str
    email: str
    pname: str
    content: str
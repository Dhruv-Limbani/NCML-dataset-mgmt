import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers.dataset import dataset_router
from routers.health import health_router

app = FastAPI()

app.include_router(dataset_router, prefix="/dataset")
app.include_router(health_router, prefix="/health")

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Hello from the Dataset Management Microservice!"}

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8002)
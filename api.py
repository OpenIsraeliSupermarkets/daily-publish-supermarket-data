from fastapi import FastAPI, HTTPException
from access_layer import AccessLayer
from remotes import DynamoDbUploader


app = FastAPI()
access_layer = AccessLayer(DynamoDbUploader)


@app.get("/list_chains/v0")
async def list_chains():
    return {"chains": access_layer.list_all_available_chains()}


@app.get("/list_file_types/v0")
async def list_file_types():
    return {"file_types": access_layer.list_all_available_file_types()}


@app.get("/list_files/v0")
async def read_files(chain: str, file_type: str = None):
    try:
        return {"files": access_layer.list_files(chain, file_type)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")


@app.get("/file_content/v0")
async def file_content(chain: str, file: str):
    try:
        return {"records": access_layer.get_file_content(chain, file)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")

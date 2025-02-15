from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from access_layer import AccessLayer, User
from remotes import MongoDbUploader
import os

app = FastAPI()
access_layer = AccessLayer(MongoDbUploader)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token/v0")

async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    user = access_layer.get_user(token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)) -> User:
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

@app.post("/register/v0")
async def register(form_data: OAuth2PasswordRequestForm = Depends()):
    if not access_layer.create_user(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    return {"message": "User created successfully"}

@app.post("/token/v0")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = access_layer.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return {"access_token": user.username, "token_type": "bearer"}

@app.get("/list_chains/v0")
async def list_chains(current_user: User = Depends(get_current_active_user)):
    return {"chains": access_layer.list_all_available_chains()}

@app.get("/list_file_types/v0")
async def list_file_types(current_user: User = Depends(get_current_active_user)):
    return {"file_types": access_layer.list_all_available_file_types()}

@app.get("/list_files/v0")
async def read_files(
    chain: str,
    file_type: str = None,
    current_user: User = Depends(get_current_active_user)
):
    try:
        return {"files": access_layer.list_files(chain, file_type)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")

@app.get("/file_content/v0")
async def file_content(
    chain: str,
    file: str,
    current_user: User = Depends(get_current_active_user)
):
    try:
        return {"records": access_layer.get_file_content(chain, file)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")

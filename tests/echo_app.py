"""Простое FastAPI echo-приложение для тестирования реверсивного прокси.

Endpoints:
    GET  /       — возвращает ``{"message": "Hello World"}``
    POST /echo   — возвращает тело запроса как ответ
"""

# Third Party
from fastapi import FastAPI, HTTPException, Request

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/echo")
async def echo(request: Request):
    body = await request.body()
    return body


@app.get("/always_500")
async def always_500():
    raise HTTPException(status_code=500, detail="Hi retry")

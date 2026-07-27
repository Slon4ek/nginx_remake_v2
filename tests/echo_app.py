"""Простое FastAPI echo-приложение для тестирования реверсивного прокси.

Endpoints:
    GET  /       — возвращает ``{"message": "Hello World"}``
    POST /echo   — возвращает тело запроса как ответ
"""

from fastapi import FastAPI, Request

app = FastAPI()


@app.get("/")
async def root():
    print("Hello World")
    return {"message": "Hello World"}


@app.post("/echo")
async def echo(request: Request):
    body = await request.body()
    return body

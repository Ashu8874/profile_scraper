import uvicorn

if __name__ == "__main__":
    uvicorn.run(
    "app.main:app",
    reload=True,
    reload_dirs=["app"],
    host="127.0.0.1",
    port=1234,
)


import uvicorn
from app.core.config import APP_HOST, APP_PORT, APP_RELOAD

if __name__ == "__main__":
    run_kwargs = {
        "app": "app.main:app",
        "reload": APP_RELOAD,
        "host": APP_HOST,
        "port": APP_PORT,
    }
    if APP_RELOAD:
        run_kwargs["reload_dirs"] = ["app"]

    uvicorn.run(**run_kwargs)

from src.main import build_app


if __name__ == "__main__":
    app = build_app()
    app.run_job("dev_history_default")
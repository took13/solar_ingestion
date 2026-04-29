from src.main import build_app


def main():
    app = build_app()
    app.normalize_plant_realtime()


if __name__ == "__main__":
    main()
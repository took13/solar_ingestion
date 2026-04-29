from src.main import build_app


def main():
    app = build_app()
    app.normalize_device_realtime()


if __name__ == "__main__":
    main()
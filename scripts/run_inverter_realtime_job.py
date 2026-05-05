from src.main import build_app


def main():
    app = build_app()
    print("[APP] Starting job from DB: inverter_realtime_online")
    app.run_job("inverter_realtime_online")
    print("[APP] Finished job: inverter_realtime_online")


if __name__ == "__main__":
    main()
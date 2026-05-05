from datetime import datetime, timezone

from src.main import build_app


def main():
    app = build_app()
    conn = app.conn
    cursor = conn.cursor()

    print("[MART] Starting device 5min mart load")

    sql = """
    DECLARE @FromUtc datetime2(0) = DATEADD(hour, -4, SYSUTCDATETIME());
    DECLARE @ToUtc   datetime2(0) = DATEADD(minute, 10, SYSUTCDATETIME());

    EXEC mart.usp_load_fact_dev_meter_5min
        @FromUtc = @FromUtc,
        @ToUtc   = @ToUtc;

    EXEC mart.usp_load_fact_dev_emi_5min
        @FromUtc = @FromUtc,
        @ToUtc   = @ToUtc;
    """

    cursor.execute(sql)

    # Consume all result sets/messages from stored procedures
    while cursor.nextset():
        pass

    conn.commit()

    print(f"[MART] Completed device 5min mart load at {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
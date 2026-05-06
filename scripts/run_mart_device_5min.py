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

    PRINT CONCAT('[MART] Window FromUtc=', CONVERT(varchar(19), @FromUtc, 120),
                 ' ToUtc=', CONVERT(varchar(19), @ToUtc, 120));

    EXEC mart.usp_load_fact_dev_meter_5min
        @FromUtc = @FromUtc,
        @ToUtc   = @ToUtc;

    PRINT '[MART] Completed meter 5min load';

    EXEC mart.usp_load_fact_dev_emi_5min
        @FromUtc = @FromUtc,
        @ToUtc   = @ToUtc;

    PRINT '[MART] Completed EMI 5min load';

    EXEC mart.usp_load_fact_dev_inverter_5min
        @FromUtc = @FromUtc,
        @ToUtc   = @ToUtc;

    PRINT '[MART] Completed inverter 5min load';
    """

    cursor.execute(sql)

    # Consume all result sets/messages from stored procedures
    while cursor.nextset():
        pass

    conn.commit()

    print(f"[MART] Completed device 5min mart load at {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
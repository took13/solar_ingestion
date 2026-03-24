class StatusService:
    def __init__(self, conn):
        self.conn = conn

    def mark_success(self, raw_id: int, generic_row_count: int):
        cursor = self.conn.cursor()
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM norm.raw_normalization_status WHERE raw_id = ?)
            BEGIN
                UPDATE norm.raw_normalization_status
                SET generic_status = 'SUCCESS',
                    generic_row_count = ?,
                    error_message = NULL,
                    updated_at_utc = SYSUTCDATETIME()
                WHERE raw_id = ?
            END
            ELSE
            BEGIN
                INSERT INTO norm.raw_normalization_status (
                    raw_id, generic_status, typed_status, generic_row_count, typed_row_count, error_message, updated_at_utc
                )
                VALUES (?, 'SUCCESS', 'SKIPPED', ?, 0, NULL, SYSUTCDATETIME())
            END
        """, (
            raw_id,
            generic_row_count,
            raw_id,
            raw_id,
            generic_row_count,
        ))
        self.conn.commit()

    def mark_failed(self, raw_id: int, error_message: str):
        cursor = self.conn.cursor()
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM norm.raw_normalization_status WHERE raw_id = ?)
            BEGIN
                UPDATE norm.raw_normalization_status
                SET generic_status = 'FAILED',
                    error_message = ?,
                    updated_at_utc = SYSUTCDATETIME()
                WHERE raw_id = ?
            END
            ELSE
            BEGIN
                INSERT INTO norm.raw_normalization_status (
                    raw_id, generic_status, typed_status, generic_row_count, typed_row_count, error_message, updated_at_utc
                )
                VALUES (?, 'FAILED', 'SKIPPED', 0, 0, ?, SYSUTCDATETIME())
            END
        """, (
            raw_id,
            error_message,
            raw_id,
            raw_id,
            error_message,
        ))
        self.conn.commit()
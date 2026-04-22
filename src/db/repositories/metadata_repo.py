from __future__ import annotations

from datetime import datetime


class MetadataRepository:
    def __init__(self, conn):
        self.conn = conn

    def get_account_by_id(self, account_id: int) -> dict | None:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                account_id,
                account_name,
                base_url,
                username,
                api_password,
                is_active,
                token_expire_minutes,
                login_cooldown_until,
                interface_cooldown_until,
                max_parallel_slots
            FROM dbo.dim_api_account
            WHERE account_id = ?
              AND is_active = 1
        """, (account_id,))
        row = cursor.fetchone()
        if not row:
            return None

        return {
            "account_id": row.account_id,
            "account_name": row.account_name,
            "base_url": row.base_url,
            "username": row.username,
            "api_password": row.api_password,
            "is_active": row.is_active,
            "token_expire_minutes": row.token_expire_minutes,
            "login_cooldown_until": row.login_cooldown_until,
            "interface_cooldown_until": row.interface_cooldown_until,
            "max_parallel_slots": row.max_parallel_slots,
        }

    def get_devices(self, plant_code: str, dev_type_id: int) -> list[dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                d.dev_id,
                d.dev_dn,
                d.dev_type_id,
                d.plant_code,
                d.dev_name,
                d.is_active
            FROM dbo.dim_device d
            WHERE d.plant_code = ?
              AND d.dev_type_id = ?
              AND d.is_active = 1
            ORDER BY d.dev_id
        """, (plant_code, dev_type_id))

        rows = cursor.fetchall()
        return [
            {
                "dev_id": r.dev_id,
                "dev_dn": r.dev_dn,
                "dev_type_id": r.dev_type_id,
                "plant_code": r.plant_code,
                "dev_name": r.dev_name,
                "is_active": r.is_active,
            }
            for r in rows
        ]

    def get_plant(self, plant_code: str) -> dict | None:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT plant_id, plant_code, plant_name
            FROM dbo.dim_plant
            WHERE plant_code = ?
        """, (plant_code,))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "plant_id": row.plant_id,
            "plant_code": row.plant_code,
            "plant_name": row.plant_name,
        }

    def resolve_account_for_plant(self, plant_code: str) -> dict | None:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT TOP 1
                a.account_id,
                a.account_name,
                a.base_url,
                a.username,
                a.api_password,
                a.is_active,
                a.interface_cooldown_until
            FROM dbo.plant_account_assignment paa
            INNER JOIN dbo.dim_api_account a
                ON paa.account_id = a.account_id
            WHERE paa.plant_code = ?
              AND a.is_active = 1
            ORDER BY a.account_id
        """, (plant_code,))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "account_id": row.account_id,
            "account_name": row.account_name,
            "base_url": row.base_url,
            "username": row.username,
            "api_password": row.api_password,
            "is_active": row.is_active,
            "interface_cooldown_until": row.interface_cooldown_until,
        }

    def get_active_plants_for_account(self, account_id: int) -> list[str]:
        """
        Used by plant realtime target.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT p.plant_code
            FROM dbo.dim_plant p
            INNER JOIN dbo.plant_account_assignment ap
                ON p.plant_code = ap.plant_code
            WHERE ap.account_id = ?
              AND p.is_active = 1
              AND p.plant_code IS NOT NULL
            ORDER BY p.plant_code
        """, (account_id,))
        rows = cursor.fetchall()
        return [r.plant_code for r in rows]

    def set_account_interface_cooldown(self, account_id: int, cooldown_until_utc: datetime) -> None:
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE dbo.dim_api_account
            SET interface_cooldown_until = ?
            WHERE account_id = ?
        """, (cooldown_until_utc, account_id))
        self.conn.commit()

    def clear_account_interface_cooldown(self, account_id: int) -> None:
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE dbo.dim_api_account
            SET interface_cooldown_until = NULL
            WHERE account_id = ?
        """, (account_id,))
        self.conn.commit()
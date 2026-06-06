from __future__ import annotations

from typing import Any


class InverterRealtimeSelectionRepository:
    def __init__(self, conn):
        self.conn = conn

    def list_selected_plants(self) -> list[str]:
        sql = """
            SELECT plant_code
            FROM cfg.inverter_realtime_selected_plant
            WHERE is_enabled = 1
            ORDER BY priority_no, plant_code;
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return [str(r.plant_code) for r in cursor.fetchall()]

    def list_selected_inverter_devices(self) -> list[dict[str, Any]]:
        sql = """
            SELECT DISTINCT
                d.plant_code,
                d.dev_id,
                d.dev_name,
                d.dev_dn
            FROM cfg.inverter_realtime_selected_plant p
            JOIN dbo.dim_device d
                ON d.plant_code = p.plant_code
               AND d.dev_type_id = 1
               AND d.is_active = 1
            WHERE p.is_enabled = 1
            ORDER BY
                p.priority_no,
                d.plant_code,
                d.dev_id;
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)

        return [
            {
                "plant_code": str(r.plant_code),
                "dev_id": int(r.dev_id),
                "dev_name": getattr(r, "dev_name", None),
                "dev_dn": getattr(r, "dev_dn", None),
            }
            for r in cursor.fetchall()
        ]
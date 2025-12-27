import sqlite3


def compute_daily_summary(conn: sqlite3.Connection) -> int:
    """
    Compute a naive daily summary per inverter from the cleaned telemetry table.

    This implementation is intentionally simple:
    - it deletes all existing summaries and recomputes everything
    - it groups by day derived from the event_time string prefix
    """
    cur = conn.cursor()
    cur.execute("DELETE FROM inverter_daily_summary")
    conn.commit()

    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            plant_id,
            inverter_id,
            substr(event_time, 1, 10) AS day,
            avg(ac_power_w) AS avg_ac_power_w,
            max(ac_power_w) AS max_ac_power_w,
            count(1) AS points
        FROM inverter_telemetry_clean
        WHERE ac_power_w IS NOT NULL
        GROUP BY plant_id, inverter_id, substr(event_time, 1, 10)
        """
    ).fetchall()

    inserted = 0
    for (plant_id, inverter_id, day, avg_p, max_p, points) in rows:
        cur2 = conn.cursor()
        cur2.execute(
            """
            INSERT INTO inverter_daily_summary
                (plant_id, inverter_id, day, avg_ac_power_w, max_ac_power_w, points)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (plant_id, inverter_id, day, avg_p, max_p, points),
        )
        conn.commit()
        inserted += 1

    return inserted



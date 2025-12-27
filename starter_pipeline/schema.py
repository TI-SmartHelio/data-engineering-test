import sqlite3


def create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plants (
            plant_id TEXT,
            name TEXT,
            location TEXT,
            timezone TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inverters (
            inverter_id TEXT,
            plant_id TEXT,
            vendor TEXT,
            model TEXT,
            capacity_kw REAL,
            commissioned_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inverter_telemetry_raw (
            event_id TEXT,
            plant_id TEXT,
            inverter_id TEXT,
            event_time TEXT,
            source TEXT,
            raw_json TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inverter_telemetry_clean (
            event_id TEXT,
            plant_id TEXT,
            inverter_id TEXT,
            event_time TEXT,
            ac_power_w REAL,
            dc_power_w REAL,
            energy_wh_total REAL,
            temperature_c REAL,
            status TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inverter_daily_summary (
            plant_id TEXT,
            inverter_id TEXT,
            day TEXT,
            avg_ac_power_w REAL,
            max_ac_power_w REAL,
            points INTEGER
        )
        """
    )

    conn.commit()


def drop_all(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS plants")
    cur.execute("DROP TABLE IF EXISTS inverters")
    cur.execute("DROP TABLE IF EXISTS inverter_telemetry_raw")
    cur.execute("DROP TABLE IF EXISTS inverter_telemetry_clean")
    cur.execute("DROP TABLE IF EXISTS inverter_daily_summary")
    conn.commit()



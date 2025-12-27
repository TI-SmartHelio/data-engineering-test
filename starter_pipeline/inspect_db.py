from starter_pipeline.db import connect, fetchall, get_db_path


def main() -> None:
    conn = connect(get_db_path())

    tables = [
        "plants",
        "inverters",
        "inverter_telemetry_raw",
        "inverter_telemetry_clean",
        "inverter_daily_summary",
    ]

    for t in tables:
        rows = fetchall(conn, f"SELECT COUNT(1) FROM {t}")
        print(t, "count =", rows[0][0] if rows else None)

    # Print a few rows from summaries.
    rows = fetchall(
        conn,
        "SELECT plant_id, inverter_id, day, avg_ac_power_w, max_ac_power_w, points FROM inverter_daily_summary ORDER BY plant_id, inverter_id, day LIMIT 20",
    )
    print("\ninverter_daily_summary sample:")
    for r in rows:
        print(" ", r)


if __name__ == "__main__":
    main()



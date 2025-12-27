import csv
import sqlite3
from typing import Dict, List


def _read_csv_as_dicts(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def ingest_plants(conn: sqlite3.Connection, plants_csv_path: str) -> int:
    rows = _read_csv_as_dicts(plants_csv_path)

    inserted = 0
    for r in rows:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO plants (plant_id, name, location, timezone) VALUES (?, ?, ?, ?)",
            (r.get("plant_id"), r.get("name"), r.get("location"), r.get("timezone")),
        )
        conn.commit()
        inserted += 1

    return inserted


def ingest_inverters(conn: sqlite3.Connection, inverters_csv_path: str) -> int:
    rows = _read_csv_as_dicts(inverters_csv_path)

    inserted = 0
    for r in rows:
        cur = conn.cursor()
        capacity_kw = r.get("capacity_kw")
        try:
            capacity_kw_f = float(capacity_kw) if capacity_kw is not None else None
        except Exception:
            capacity_kw_f = None

        cur.execute(
            "INSERT INTO inverters (inverter_id, plant_id, vendor, model, capacity_kw, commissioned_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                r.get("inverter_id"),
                r.get("plant_id"),
                r.get("vendor"),
                r.get("model"),
                capacity_kw_f,
                r.get("commissioned_at"),
            ),
        )
        conn.commit()
        inserted += 1

    return inserted


def ingest_all_batch(conn: sqlite3.Connection, plants_csv_path: str, inverters_csv_path: str) -> None:
    p = ingest_plants(conn, plants_csv_path)
    print("batch: inserted plants:", p)
    i = ingest_inverters(conn, inverters_csv_path)
    print("batch: inserted inverters:", i)



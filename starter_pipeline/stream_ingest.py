import json
import sqlite3
import time
from typing import Any, Dict, Optional


def _parse_event(line: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(line)
    except Exception:
        return None


def _insert_raw_row(conn: sqlite3.Connection, evt: Dict[str, Any]) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO inverter_telemetry_raw (event_id, plant_id, inverter_id, event_time, source, raw_json) VALUES (?, ?, ?, ?, ?, ?)",
        (
            evt.get("event_id"),
            evt.get("plant_id"),
            evt.get("inverter_id"),
            evt.get("event_time"),
            evt.get("source"),
            json.dumps(evt),
        ),
    )
    conn.commit()


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _convert_power_to_watts(value: Optional[float], unit: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    if unit is None:
        return value
    u = str(unit).strip().lower()
    if u == "w":
        return value
    if u == "kw":
        return value * 1000.0
    return value


def _insert_clean_row(conn: sqlite3.Connection, evt: Dict[str, Any]) -> None:
    ac_power = _to_float(evt.get("ac_power"))
    dc_power = _to_float(evt.get("dc_power"))
    energy_total = _to_float(evt.get("energy_wh_total"))
    temp_c = _to_float(evt.get("temperature_c"))

    # Unit normalization.
    ac_power_w = _convert_power_to_watts(ac_power, evt.get("ac_power_unit"))
    dc_power_w = _convert_power_to_watts(dc_power, evt.get("dc_power_unit"))

    # Basic cleaning (naive).
    if ac_power_w is not None and ac_power_w < 0:
        ac_power_w = None
    if dc_power_w is not None and dc_power_w < 0:
        dc_power_w = None

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO inverter_telemetry_clean
            (event_id, plant_id, inverter_id, event_time, ac_power_w, dc_power_w, energy_wh_total, temperature_c, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evt.get("event_id"),
            evt.get("plant_id"),
            evt.get("inverter_id"),
            evt.get("event_time"),
            ac_power_w,
            dc_power_w,
            energy_total,
            temp_c,
            evt.get("status"),
        ),
    )
    conn.commit()


def ingest_events_jsonl(
    conn: sqlite3.Connection,
    jsonl_path: str,
    *,
    max_loops: int = 3,
    sleep_seconds: float = 0.2,
) -> int:
    """
    Simulate streaming ingestion by repeatedly re-reading the entire JSONL file.
    """
    processed = 0

    loops = 0
    while True:
        loops += 1
        if loops > max_loops:
            break

        with open(jsonl_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                evt = _parse_event(line)
                if evt is None:
                    continue

                _insert_raw_row(conn, evt)
                _insert_clean_row(conn, evt)
                processed += 1

        time.sleep(sleep_seconds)

    return processed



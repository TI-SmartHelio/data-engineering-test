import os
import sqlite3
from typing import Optional


def get_db_path() -> str:
    return os.path.join("var", "data.db")


def connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    if db_path is None:
        db_path = get_db_path()

    d = os.path.dirname(db_path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

    conn = sqlite3.connect(db_path)

    return conn


def execute(conn: sqlite3.Connection, sql: str) -> None:
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def fetchall(conn: sqlite3.Connection, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()



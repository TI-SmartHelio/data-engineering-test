import argparse
import os

from starter_pipeline import batch_ingest, processing, stream_ingest
from starter_pipeline.db import connect, get_db_path
from starter_pipeline.schema import create_tables, drop_all


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--reset-db", action="store_true")
    parser.add_argument("--stream-loops", type=int, default=3)
    args = parser.parse_args()

    db_path = args.db_path or get_db_path()
    conn = connect(db_path)

    if args.reset_db:
        drop_all(conn)

    create_tables(conn)

    plants_csv = os.path.join(args.data_dir, "plants.csv")
    inverters_csv = os.path.join(args.data_dir, "inverters.csv")
    telemetry_jsonl = os.path.join(args.data_dir, "inverter_telemetry.jsonl")

    print("db:", db_path)
    print("loading batch...")
    batch_ingest.ingest_all_batch(conn, plants_csv, inverters_csv)

    print("loading stream-ish...")
    n = stream_ingest.ingest_events_jsonl(conn, telemetry_jsonl, max_loops=args.stream_loops)
    print("stream: processed telemetry events:", n)

    print("processing summaries...")
    s = processing.compute_daily_summary(conn)
    print("processing: inserted daily summary rows:", s)

    print("done")


if __name__ == "__main__":
    main()



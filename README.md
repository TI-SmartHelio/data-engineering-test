# Data Engineer II Onsite Coding Exercise

This repo contains a starter implementation of a small data platform slice:

- **Batch ingestion**: `plants.csv`, `inverters.csv`
- **Stream-ish ingestion**: `inverter_telemetry.jsonl` (simulated event stream)
- **Storage**: SQLite database file (`./var/data.db`)

The code runs locally and loads data into SQLite. Your task is to improve it toward production-grade quality.

## What you need to build (what we evaluate)

You will improve the existing implementation to be:

- **Correct**: handles duplicates, retries, partial failures, out-of-order events, and late-arriving updates.
- **Reliable**: idempotent ingestion (safe re-runs), clear error handling, and safe DB writes.
- **Maintainable**: clean abstractions, less repetition, better module boundaries, type hints, linting-friendly code.
- **Tested**: add unit + integration tests for core behaviors.
- **Observable**: structured logs and basic metrics counters (even if just logged).
- **Documented**: short design notes and “how to run / how to recover” runbook.

## Constraints (onsite)

- Use **Python 3.11+**
- Keep it runnable locally (SQLite is OK)
- Prefer standard library; lightweight dependencies are fine if justified

## Dataset & Domain

- `plants.csv`: solar plant registry
- `inverters.csv`: inverter registry (per plant)
- `inverter_telemetry.jsonl`: inverter telemetry events (time-series)

In reality, streams can be out-of-order and duplicated. Assume:

- the same event can arrive multiple times
- events can arrive out of order (`event_time` not sorted)
- you may re-run the pipeline and it must not corrupt results

## Quickstart

Create venv and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the pipeline:

```bash
python3 -m starter_pipeline.run_all --reset-db
```

Inspect the DB:

```bash
python3 -m starter_pipeline.inspect_db
```

## Starter code map

- `starter_pipeline/run_all.py`: runs batch load + “stream” load
- `starter_pipeline/batch_ingest.py`: loads CSVs
- `starter_pipeline/stream_ingest.py`: simulates a stream by repeatedly reading JSONL
- `starter_pipeline/db.py`: SQLite helpers
- `starter_pipeline/schema.py`: table creation

## Your tasks (pick a good path, explain trade-offs)

Minimum expected improvements:

1. **Idempotency & dedupe**
   - Make batch loads safe to re-run
   - Prevent duplicate telemetry events and duplicate cleaned rows
2. **Preprocessing & correctness**
   - Define and enforce a data contract for telemetry (types, required fields, unit normalization)
   - Handle out-of-order updates correctly (based on `event_time`)
3. **Processing**
   - Produce a correct daily summary per inverter in `inverter_daily_summary`
3. **Testing**
   - Unit tests around transforms / ordering / dedupe rules
   - Integration test that runs pipeline on sample data and asserts final tables
4. **Observability**
   - Structured logs + counters for processed/failed/skipped
5. **Refactor**
   - Reduce duplication, improve interfaces, type hints

Nice-to-have:

- Basic checkpointing (don’t re-read entire stream each run)
- Performance improvements (avoid O(N^2) patterns, bulk inserts)
- Config management (env vars / CLI)

## Evaluation rubric (what we look for)

- **Correctness** (40%): edge cases, ordering, dedupe, safe re-runs
- **Code quality** (25%): readability, structure, typing, API design
- **Testing** (20%): quality/coverage of tests and fixtures
- **Operability** (10%): logs/metrics, runbook, failure recovery story
- **Performance** (5%): sensible optimizations and measurement



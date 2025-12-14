#!/usr/bin/env python
"""Load Retailrocket CSV drops into the raw schema with optional chunked daily loads."""
import argparse
import csv
import datetime as dt
import logging
import os
from dataclasses import dataclass
from pathlib import Path
import tempfile
from typing import Dict, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extensions import connection as _connection


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@dataclass(frozen=True)
class RawFile:
    name: str
    filename: str
    table: str
    columns: Sequence[str]
    incremental: bool = True  # participates in daily chunk loads


RAW_FILES: Sequence[RawFile] = (
    RawFile(
        name="events",
        filename="events.csv",
        table="raw.events",
        columns=("timestamp", "visitorid", "event", "itemid", "transactionid"),
    ),
    RawFile(
        name="item_properties_part1",
        filename="item_properties_part1.csv",
        table="raw.item_properties",
        columns=("timestamp", "itemid", "property", "value"),
    ),
    RawFile(
        name="item_properties_part2",
        filename="item_properties_part2.csv",
        table="raw.item_properties",
        columns=("timestamp", "itemid", "property", "value"),
    ),
    RawFile(
        name="category_tree",
        filename="category_tree.csv",
        table="raw.category_tree",
        columns=("categoryid", "parentid"),
        incremental=False,
    ),
)


DDL_BY_TABLE: Dict[str, str] = {
    "raw.events": """
        CREATE TABLE IF NOT EXISTS raw.events (
            "timestamp" BIGINT,
            visitorid TEXT,
            event TEXT,
            itemid BIGINT,
            transactionid TEXT,
            ingestion_date DATE NOT NULL
        );
    """,
    "raw.item_properties": """
        CREATE TABLE IF NOT EXISTS raw.item_properties (
            "timestamp" BIGINT,
            itemid BIGINT,
            property TEXT,
            value TEXT,
            ingestion_date DATE NOT NULL
        );
    """,
    "raw.category_tree": """
        CREATE TABLE IF NOT EXISTS raw.category_tree (
            categoryid BIGINT,
            parentid BIGINT,
            ingestion_date DATE NOT NULL
        );
    """,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest archive CSV files into raw schema.")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("archive"),
        help="Directory containing CSV files (default: archive)",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "daily"),
        default="full",
        help="Full loads every row; daily ingests the next chunk of rows per file.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Ingestion date (YYYY-MM-DD). Required for daily mode; defaults to today for full loads.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=150_000,
        help="Number of rows to ingest per file in daily mode (default: 150000).",
    )
    parser.add_argument(
        "--pg-host",
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="Postgres host (default: localhost)",
    )
    parser.add_argument(
        "--pg-port",
        default=int(os.getenv("POSTGRES_PORT", 5432)),
        type=int,
        help="Postgres port (default: 5432)",
    )
    parser.add_argument(
        "--pg-user",
        default=os.getenv("POSTGRES_USER", "airflow"),
        help="Postgres user (default: airflow)",
    )
    parser.add_argument(
        "--pg-password",
        default=os.getenv("POSTGRES_PASSWORD", "airflow"),
        help="Postgres password (default: airflow)",
    )
    parser.add_argument(
        "--pg-db",
        default=os.getenv("POSTGRES_DB", "airflow"),
        help="Postgres database (default: airflow)",
    )
    parser.add_argument(
        "--skip-static",
        action="store_true",
        help="Skip non-incremental files such as category_tree.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Do not delete temporary files (debug aid).",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Reset chunk ingestion state before running (daily mode).",
    )
    return parser.parse_args()


def ensure_tables(conn: _connection) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        for table, ddl in DDL_BY_TABLE.items():
            logging.debug("Ensuring table %s exists", table)
            cur.execute(ddl)
            cur.execute(f"ALTER TABLE {table} ALTER COLUMN ingestion_date DROP DEFAULT;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.ingestion_state (
                file_name TEXT PRIMARY KEY,
                last_row BIGINT DEFAULT 0,
                completed BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
    conn.commit()


def parse_ingestion_date(args: argparse.Namespace) -> dt.date:
    if args.mode == "daily" and not args.date:
        raise ValueError("--date is required in daily mode")
    if args.date:
        return dt.date.fromisoformat(args.date)
    return dt.date.today()


def load_state(conn: _connection, reset: bool) -> Dict[str, Tuple[int, bool]]:
    with conn.cursor() as cur:
        if reset:
            cur.execute("TRUNCATE TABLE raw.ingestion_state;")
        cur.execute("SELECT file_name, last_row, completed FROM raw.ingestion_state;")
        existing = cur.fetchall()
    return {row[0]: (row[1], row[2]) for row in existing}


def save_state(conn: _connection, file_name: str, last_row: int, completed: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.ingestion_state (file_name, last_row, completed, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (file_name) DO UPDATE
            SET last_row = EXCLUDED.last_row,
                completed = EXCLUDED.completed,
                updated_at = NOW();
            """,
            (file_name, last_row, completed),
        )


def prepare_rows(
    cfg: RawFile,
    source: Path,
    ingestion_date: dt.date,
    start_row: int = 0,
    max_rows: Optional[int] = None,
) -> Tuple[Optional[Path], int, bool, int]:
    """Copy the required slice into a temp CSV with ingestion_date appended."""
    if max_rows is not None and max_rows <= 0:
        return None, 0, False, start_row

    tmp = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="")
    writer = csv.writer(tmp)
    writer.writerow([*cfg.columns, "ingestion_date"])

    written = 0
    reached_eof = True
    last_row_index = start_row
    with source.open("r", encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        header = next(reader, None)
        if header is None:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            return None, 0, True, start_row
        idx_map = [header.index(col) for col in cfg.columns]

        for idx, row in enumerate(reader):
            if idx < start_row:
                continue
            if max_rows is not None and written >= max_rows:
                reached_eof = False
                last_row_index = start_row + written
                break
            values = [row[i] if i < len(row) else "" for i in idx_map]
            writer.writerow(values + [ingestion_date.isoformat()])
            written += 1
            last_row_index = idx + 1
        else:
            reached_eof = True
            last_row_index = max(start_row, last_row_index)

    tmp.close()
    if written == 0:
        Path(tmp.name).unlink(missing_ok=True)
        return None, 0, reached_eof, last_row_index
    return Path(tmp.name), written, reached_eof, last_row_index


def copy_file(
    conn: _connection,
    cfg: RawFile,
    prepared_csv: Path,
    ingestion_date: dt.date,
    truncate_tracker: Dict[str, bool],
    rows_prepared: int,
    mode: str,
) -> int:
    table = cfg.table
    needs_truncate = mode == "full" and not truncate_tracker.get(table)
    needs_delete = mode == "daily" and not truncate_tracker.get(table)

    with conn.cursor() as cur, prepared_csv.open("r", encoding="utf-8") as fh:
        if needs_truncate:
            logging.info("Truncating %s before load", table)
            cur.execute(f"TRUNCATE TABLE {table};")
            truncate_tracker[table] = True
        elif needs_delete:
            logging.info("Removing rows for %s from %s", ingestion_date, table)
            cur.execute(f"DELETE FROM {table} WHERE ingestion_date = %s;", (ingestion_date,))
            truncate_tracker[table] = True

        column_sql = ", ".join(f'"{col}"' for col in (*cfg.columns, "ingestion_date"))
        copy_sql = f'COPY {table} ({column_sql}) FROM STDIN WITH CSV HEADER'
        logging.info("Loading %s rows from %s into %s", rows_prepared, prepared_csv.name, table)
        fh.seek(0)
        cur.copy_expert(copy_sql, fh)
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE ingestion_date = %s;", (ingestion_date,))
        loaded = cur.fetchone()[0]
        logging.info("Rows in %s for %s: %s", table, ingestion_date, loaded)
        return loaded


def ingest(args: argparse.Namespace) -> None:
    ingestion_date = parse_ingestion_date(args)
    source_dir = args.source_dir.resolve()
    if not source_dir.exists():
        raise FileNotFoundError(f"Source dir {source_dir} not found")

    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
    )
    temp_files: List[Path] = []
    try:
        ensure_tables(conn)
        state = load_state(conn, args.reset_state)
        total_loaded = 0
        table_tracker: Dict[str, bool] = {}

        for cfg in RAW_FILES:
            src_path = source_dir / cfg.filename
            if not src_path.exists():
                logging.warning("Skipping %s; %s missing", cfg.name, src_path)
                continue
            if args.mode == "daily" and not cfg.incremental:
                logging.info("Skipping %s in daily mode (static dataset)", cfg.name)
                continue
            if args.skip_static and not cfg.incremental:
                logging.info("Skipping static file %s due to --skip-static", cfg.name)
                continue

            start_row, completed = state.get(cfg.name, (0, False))
            if args.mode == "daily" and completed:
                logging.info("%s already fully ingested; skipping", cfg.name)
                continue

            max_rows = None if args.mode == "full" else args.chunk_size
            prepared, rows_written, reached_eof, last_row = prepare_rows(
                cfg,
                src_path,
                ingestion_date,
                start_row=start_row if args.mode == "daily" else 0,
                max_rows=max_rows,
            )
            if prepared is None or rows_written == 0:
                logging.info("No rows prepared for %s (start=%s)", cfg.name, start_row)
                if args.mode == "daily":
                    save_state(conn, cfg.name, last_row, True)
                continue

            temp_files.append(prepared)
            loaded = copy_file(conn, cfg, prepared, ingestion_date, table_tracker, rows_written, args.mode)
            total_loaded += loaded

            if args.mode == "daily":
                save_state(conn, cfg.name, last_row, reached_eof)

        conn.commit()
        logging.info("Ingestion complete for %s. Rows loaded: %s", ingestion_date, total_loaded)
    except Exception:
        conn.rollback()
        raise
    finally:
        if not args.keep_temp:
            for tmp in temp_files:
                Path(tmp).unlink(missing_ok=True)
        conn.close()


if __name__ == "__main__":
    ingest(parse_args())

"""ETL pipeline for the TSETMC market map into PostgreSQL."""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from set_logger import get_logger

logger = get_logger(__name__)

TSETMC_URL = (
    "https://cdn.tsetmc.com/api/ClosingPrice/"
    "GetMarketMap?market=0&size=1366&sector=0&typeSelected=1&hEven=0"
)
REQUEST_TIMEOUT = (5, 30)
DROP_COLUMNS = ["fontColor", "fontSize", "hEven", "lVal30", "customLabel"]
OUTPUT_COLUMNS = [
    "code", "date", "close", "last", "number_trade", "volume", "value",
    "yesterday", "name", "sector", "percent", "percent_last", "time", "color",
]


def get_database_engine() -> Engine:
    """Build a database engine from environment variables."""
    names = (
        "postgres_host",
        "postgres_database",
        "postgres_user",
        "postgres_password",
        "postgres_port",
    )
    settings = {name: os.environ.get(name) for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError(
            "Missing PostgreSQL environment variable(s): " + ", ".join(missing)
        )

    try:
        port = int(settings["postgres_port"])
    except (TypeError, ValueError) as exc:
        raise RuntimeError("postgres_port must be an integer") from exc

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=settings["postgres_user"],
        password=settings["postgres_password"],
        host=settings["postgres_host"],
        port=port,
        database=settings["postgres_database"],
    )
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800)


def extract_data() -> pd.DataFrame:
    """Fetch the latest market map and return its rows as a DataFrame."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    try:
        response = requests.get(
            TSETMC_URL, headers=headers, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        payload: Any = response.json()
    except requests.RequestException:
        logger.exception("Request to TSETMC failed.")
        raise
    except ValueError:
        logger.exception("TSETMC returned invalid JSON.")
        raise

    if isinstance(payload, dict):
        # Support common API response envelopes while keeping the expected row shape.
        for key in ("marketMap", "data", "items", "dataList"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break

    if not isinstance(payload, list):
        raise ValueError(
            f"Unexpected TSETMC response: expected a JSON list, got "
            f"{type(payload).__name__}"
        )
    if not payload:
        raise ValueError("TSETMC returned an empty market map; refusing to load it.")

    df = pd.DataFrame(payload)
    logger.info("Extracted %s rows from TSETMC.", len(df))
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize the TSETMC market-map columns."""
    if df is None or df.empty:
        raise ValueError("Cannot transform an empty DataFrame.")

    result = df.copy()
    missing = [column for column in DROP_COLUMNS if column not in result.columns]
    if missing:
        raise ValueError(
            "TSETMC response schema changed; missing expected column(s): "
            + ", ".join(missing)
        )

    result.drop(columns=DROP_COLUMNS, inplace=True)
    if len(result.columns) != len(OUTPUT_COLUMNS):
        raise ValueError(
            "Unexpected TSETMC schema: expected "
            f"{len(OUTPUT_COLUMNS)} columns after cleanup, got {len(result.columns)}. "
            f"Columns received: {list(result.columns)}"
        )

    result.columns = OUTPUT_COLUMNS
    logger.info("Transformed %s rows.", len(result))
    return result


def load_history_data(df: pd.DataFrame, engine: Engine, table_name: str) -> int:
    """Upsert one daily market snapshot per instrument into a history table.

    Rows are unique by (code, snapshot_date). Re-running the ETL on the same
    day updates that day's row instead of creating duplicates. The history
    table is separate from the default snapshot table.
    """
    if "code" not in df.columns:
        raise ValueError("History mode requires the instrument 'code' column.")

    snapshot_at = datetime.now(timezone.utc)
    history_df = df.copy()
    history_df["snapshot_at"] = snapshot_at
    history_df["snapshot_date"] = snapshot_at.date()
    history_df = history_df.drop_duplicates(subset=["code"], keep="last")

    columns = list(history_df.columns)
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    select_columns = quoted_columns
    update_columns = [column for column in columns if column not in {"code", "snapshot_date"}]
    update_clause = ", ".join(
        f'"{column}" = EXCLUDED."{column}"' for column in update_columns
    )
    index_name = f"uq_{table_name}_code_snapshot_date"[:60]
    staging_table = f"_etl_stage_{uuid.uuid4().hex[:12]}"

    # Identifiers are validated by load_data before reaching this function.
    with engine.begin() as connection:
        # Create an empty table from the DataFrame schema only when it is new.
        history_df.head(0).to_sql(
            name=table_name, con=connection, if_exists="append", index=False
        )
        connection.execute(text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" '
            f'ON "{table_name}" ("code", "snapshot_date")'
        ))
        history_df.to_sql(
            name=staging_table,
            con=connection,
            if_exists="fail",
            index=False,
            chunksize=1000,
            method="multi",
        )
        try:
            result = connection.execute(text(
                f'INSERT INTO "{table_name}" ({quoted_columns}) '
                f'SELECT {select_columns} FROM "{staging_table}" '
                f'ON CONFLICT ("code", "snapshot_date") DO UPDATE '
                f'SET {update_clause}'
            ))
            rows_written = int(result.rowcount or 0)
        finally:
            connection.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))

    logger.info(
        "Upserted %s daily instrument rows into history table '%s'.",
        rows_written, table_name,
    )
    return rows_written


def load_data(
    df: pd.DataFrame,
    db_engine: Engine | None = None,
    *,
    table_name: str | None = None,
    if_exists: str | None = None,
) -> int:
    """Load the latest snapshot or upsert a daily history snapshot.

    TSE_STORAGE_MODE=snapshot (default) preserves the original replace behavior.
    TSE_STORAGE_MODE=history writes to a separate table and upserts one row per
    instrument per UTC calendar day. History mode defaults to 'tse_history'.
    """
    if df is None or df.empty:
        raise ValueError("Refusing to load an empty DataFrame.")

    engine = db_engine or get_database_engine()
    storage_mode = os.getenv("TSE_STORAGE_MODE", "snapshot").lower()
    target_table = table_name or os.getenv(
        "TSE_TABLE_NAME", "tse_history" if storage_mode == "history" else "tse"
    )

    if not target_table.replace("_", "").isalnum():
        raise ValueError("TSE_TABLE_NAME may contain only letters, numbers, and underscores.")

    if storage_mode == "history":
        return load_history_data(df, engine, target_table)
    if storage_mode != "snapshot":
        raise ValueError("TSE_STORAGE_MODE must be 'snapshot' or 'history'.")

    write_mode = if_exists or os.getenv("TSE_IF_EXISTS", "replace")
    if write_mode not in {"fail", "replace", "append"}:
        raise ValueError("TSE_IF_EXISTS must be one of: fail, replace, append.")

    rows_written = df.to_sql(
        name=target_table,
        con=engine,
        if_exists=write_mode,
        index=False,
        chunksize=1000,
        method="multi",
    )
    count = int(rows_written or 0)
    logger.info(
        "Loaded %s rows into PostgreSQL table '%s' (if_exists=%s).",
        count, target_table, write_mode,
    )
    return count


def etl() -> int:
    """Run extract, transform, and load; propagate failures to the process."""
    logger.info("ETL run started.")
    engine = None
    try:
        raw_df = extract_data()
        clean_df = transform_data(raw_df)
        engine = get_database_engine()
        rows_written = load_data(clean_df, db_engine=engine)
        logger.info("ETL run completed successfully.")
        return rows_written
    except Exception:
        logger.exception("ETL run failed.")
        raise
    finally:
        if engine is not None:
            engine.dispose()


def main() -> int:
    try:
        etl()
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

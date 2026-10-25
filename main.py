"""ETL pipeline for the TSETMC market map into PostgreSQL."""

from __future__ import annotations

import os
import sys
from typing import Any

import pandas as pd
import requests
from sqlalchemy import URL, create_engine
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


def load_data(
    df: pd.DataFrame,
    db_engine: Engine | None = None,
    *,
    table_name: str | None = None,
    if_exists: str | None = None,
) -> int:
    """Load a market snapshot into PostgreSQL.

    The default 'replace' mode preserves the original snapshot behavior.
    Set TSE_IF_EXISTS=append to retain previous snapshots (add a timestamp/key
    strategy before using append for repeated scheduled runs).
    """
    if df is None or df.empty:
        raise ValueError("Refusing to load an empty DataFrame.")

    engine = db_engine or get_database_engine()
    target_table = table_name or os.getenv("TSE_TABLE_NAME", "tse")
    write_mode = if_exists or os.getenv("TSE_IF_EXISTS", "replace")

    if not target_table.replace("_", "").isalnum():
        raise ValueError("TSE_TABLE_NAME may contain only letters, numbers, and underscores.")
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

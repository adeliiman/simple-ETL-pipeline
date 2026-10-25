# simple-ETL-pipeline

A small Python ETL job that extracts the current market map from the TSETMC API, validates and normalizes the response, and loads it into PostgreSQL.

## Pipeline

1. **Extract** — request the TSETMC market map over HTTPS with a bounded timeout.
2. **Transform** — validate the expected response structure and normalize columns.
3. **Load** — write the current snapshot to PostgreSQL.
4. **Observe** — log progress and errors to stdout and a rotating `ETL.log` file.

## Requirements

- Python 3.10+
- PostgreSQL
- PostgreSQL credentials supplied through environment variables

Install dependencies in a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Configuration

Set these environment variables before running the job:

| Variable | Required | Description |
|---|---|---|
| `postgres_host` | Yes | PostgreSQL hostname |
| `postgres_database` | Yes | Database name |
| `postgres_user` | Yes | Database username |
| `postgres_password` | Yes | Database password |
| `postgres_port` | Yes | PostgreSQL port, usually `5432` |
| `TSE_TABLE_NAME` | No | Destination table; defaults to `tse` |
| `TSE_IF_EXISTS` | No | Snapshot mode only: `replace` (default), `append`, or `fail` |
| `TSE_STORAGE_MODE` | No | `snapshot` (default) or `history` |
| `ETL_LOG_FILE` | No | Log file path; defaults to `ETL.log` |
| `ETL_LOG_LEVEL` | No | Logging level; defaults to `INFO` |

For example, export credentials in your shell or load them from a local, untracked environment file. Do not commit real credentials to Git.

`TSE_STORAGE_MODE=snapshot` intentionally preserves the original project's behavior: the destination table is replaced on each successful run. To retain daily history, set `TSE_STORAGE_MODE=history`; it writes to `tse_history` by default (or `TSE_TABLE_NAME` if set), adds UTC `snapshot_at` and `snapshot_date` fields, and upserts on `(code, snapshot_date)`. Re-running the job on the same UTC date updates that date's instrument row instead of duplicating it. History mode uses PostgreSQL-specific `ON CONFLICT` and does not use `TSE_IF_EXISTS`. Use a dedicated history table; an existing table with an incompatible schema will fail rather than be silently rewritten.

## Tests and continuous integration

Install the development dependencies and run the unit tests locally:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

GitHub Actions runs the unit-test suite on Python 3.10, 3.11, and 3.12 for pushes to the main/improvement branches and for pull requests targeting `main`. The tests mock HTTP and database boundaries; they do not require live TSETMC access or PostgreSQL credentials.

## Run

```bash
source venv/bin/activate
python main.py
```

Or run the helper script:

```bash
chmod +x script.sh
./script.sh
```

The process exits with status code `0` on success and a non-zero status on failure, which makes it suitable for cron and monitoring.

## Cron example

Run once after the market closes (adjust the time and path for your environment):

```cron
30 13 * * 1-5 /absolute/path/to/simple-ETL-pipeline/script.sh >> /absolute/path/to/simple-ETL-pipeline/cron.log 2>&1
```

Cron uses the environment available to the cron process, not necessarily the variables in an interactive shell. Configure credentials securely for the service/user running the job.

## Known limitations

- The transform step intentionally checks the current expected TSETMC schema. If TSETMC changes its response columns, the job fails with a clear error instead of silently mapping incorrect data.
- History mode retains one daily snapshot per instrument, not intraday ticks. The deduplication date is UTC; if you need Tehran-market-date semantics or multiple intraday snapshots, adapt the date/key policy before production use.
- API availability and response structure are controlled by TSETMC and may change.

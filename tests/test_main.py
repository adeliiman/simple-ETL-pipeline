from __future__ import annotations

import pandas as pd
import pytest

import main


def sample_raw_frame() -> pd.DataFrame:
    # Five fields removed by transform_data plus fourteen output fields.
    columns = main.DROP_COLUMNS + [f"source_{index}" for index in range(14)]
    return pd.DataFrame([{column: index for index, column in enumerate(columns)}])


def test_transform_data_renames_columns_without_mutating_input():
    raw = sample_raw_frame()
    original_columns = list(raw.columns)

    transformed = main.transform_data(raw)

    assert list(transformed.columns) == main.OUTPUT_COLUMNS
    assert len(transformed) == 1
    assert list(raw.columns) == original_columns


def test_transform_data_rejects_empty_frame():
    with pytest.raises(ValueError, match="empty DataFrame"):
        main.transform_data(pd.DataFrame())


def test_transform_data_rejects_schema_drift():
    raw = sample_raw_frame().drop(columns=[main.DROP_COLUMNS[0]])

    with pytest.raises(ValueError, match="missing expected column"):
        main.transform_data(raw)


def test_extract_data_uses_timeout_and_returns_frame(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return [{"instrument": "sample"}]

    calls = {}

    def fake_get(url, **kwargs):
        calls["url"] = url
        calls.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(main.requests, "get", fake_get)

    result = main.extract_data()

    assert result.to_dict(orient="records") == [{"instrument": "sample"}]
    assert calls["url"].startswith("https://")
    assert calls["timeout"] == main.REQUEST_TIMEOUT


def test_extract_data_rejects_empty_payload(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return []

    monkeypatch.setattr(main.requests, "get", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(ValueError, match="empty market map"):
        main.extract_data()


def test_get_database_engine_requires_all_environment_variables(monkeypatch):
    for key in (
        "postgres_host",
        "postgres_database",
        "postgres_user",
        "postgres_password",
        "postgres_port",
    ):
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(RuntimeError, match="Missing PostgreSQL"):
        main.get_database_engine()


def test_load_data_rejects_empty_frame():
    with pytest.raises(ValueError, match="empty DataFrame"):
        main.load_data(pd.DataFrame(), db_engine=object())


def test_load_data_validates_table_name_and_mode():
    frame = pd.DataFrame({"code": ["ABC"]})

    with pytest.raises(ValueError, match="TSE_TABLE_NAME"):
        main.load_data(frame, db_engine=object(), table_name="tse; DROP TABLE tse")

    with pytest.raises(ValueError, match="TSE_IF_EXISTS"):
        main.load_data(frame, db_engine=object(), if_exists="upsert")


def test_load_data_passes_expected_options(monkeypatch):
    frame = pd.DataFrame({"code": ["ABC"]})
    captured = {}

    def fake_to_sql(self, **kwargs):
        captured.update(kwargs)
        return 1

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    count = main.load_data(
        frame,
        db_engine=object(),
        table_name="tse_test",
        if_exists="append",
    )

    assert count == 1
    assert captured["name"] == "tse_test"
    assert captured["if_exists"] == "append"
    assert captured["index"] is False



def test_history_mode_uses_history_table_and_upsert_helper(monkeypatch):
    frame = pd.DataFrame({"code": ["ABC"], "close": [100]})
    captured = {}

    def fake_history(data, engine, table_name):
        captured["data"] = data
        captured["engine"] = engine
        captured["table_name"] = table_name
        return 1

    monkeypatch.setenv("TSE_STORAGE_MODE", "history")
    monkeypatch.delenv("TSE_TABLE_NAME", raising=False)
    monkeypatch.setattr(main, "load_history_data", fake_history)
    engine = object()

    count = main.load_data(frame, db_engine=engine)

    assert count == 1
    assert captured["engine"] is engine
    assert captured["table_name"] == "tse_history"
    assert captured["data"].equals(frame)


def test_load_data_rejects_unknown_storage_mode(monkeypatch):
    monkeypatch.setenv("TSE_STORAGE_MODE", "archive")
    frame = pd.DataFrame({"code": ["ABC"]})

    with pytest.raises(ValueError, match="TSE_STORAGE_MODE"):
        main.load_data(frame, db_engine=object(), table_name="tse_test")

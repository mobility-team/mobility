import pandas as pd

from mobility.spatial.selected_rows import read_selected_rows


class FakeAsset:
    def __init__(self, rows, update_needed, cache_path="cache.parquet"):
        self._rows = rows
        self._update_needed = update_needed
        self.cache_path = cache_path

    def is_update_needed(self):
        return self._update_needed

    def get(self):
        return self._rows.copy()


def test_read_selected_rows_returns_empty_table_for_no_ids():
    rows = pd.DataFrame({"local_admin_unit_id": ["fr-1"], "value": [1]})

    selected = read_selected_rows(
        FakeAsset(rows, update_needed=True),
        id_column="local_admin_unit_id",
        ids=[],
        empty_columns=["local_admin_unit_id", "value"],
    )

    assert list(selected.columns) == ["local_admin_unit_id", "value"]
    assert selected.empty


def test_read_selected_rows_uses_asset_rows_when_update_is_needed():
    rows = pd.DataFrame({"local_admin_unit_id": ["fr-1", "fr-2"], "value": [1, 2]})

    selected = read_selected_rows(
        FakeAsset(rows, update_needed=True),
        id_column="local_admin_unit_id",
        ids=["fr-2", "fr-2"],
        empty_columns=["local_admin_unit_id", "value"],
    )

    assert selected.to_dict("records") == [{"local_admin_unit_id": "fr-2", "value": 2}]


def test_read_selected_rows_falls_back_when_filtered_parquet_read_is_not_supported(monkeypatch):
    rows = pd.DataFrame({"local_admin_unit_id": ["fr-1", "fr-2"], "value": [1, 2]})
    read_calls = []

    def fake_read_parquet(path, filters=None):
        read_calls.append(filters)
        if filters is not None:
            raise TypeError("filters not supported")
        return rows

    monkeypatch.setattr("mobility.spatial.selected_rows.pd.read_parquet", fake_read_parquet)

    selected = read_selected_rows(
        FakeAsset(rows, update_needed=False),
        id_column="local_admin_unit_id",
        ids=["fr-2"],
        empty_columns=["local_admin_unit_id", "value"],
    )

    assert read_calls == [[("local_admin_unit_id", "in", ["fr-2"])], None]
    assert selected.to_dict("records") == [{"local_admin_unit_id": "fr-2", "value": 2}]

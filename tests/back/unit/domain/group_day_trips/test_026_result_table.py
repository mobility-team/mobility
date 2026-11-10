"""Check the lifetime and saving of temporary raw result tables."""

from pathlib import Path

import polars as pl
import pandas as pd
import pytest
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.results.result_table import ResultTable
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.plans.mode_sequence_search.mode_sequences import ModeSequences
from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import asset_resolution_context
from mobility.runtime.temporary import (
    TemporaryDiskLimitExceeded, managed_temporary_disk_usage, temporary_file_writer,
    check_temporary_disk_limit, write_temporary_parquet,
)


def test_result_table_views_share_lifetime_and_can_be_saved(tmp_path):
    table = ResultTable()
    directory = table._directory.name
    table._append(pl.DataFrame({"step": [1, 1], "value": [3, 1]}).lazy())
    table._append(pl.DataFrame({"step": [2], "value": [2]}))
    selected = table.filter(pl.col("step") == 1).sort("value").select("value")
    expected = pl.DataFrame({"value": [1, 3]})
    assert_frame_equal(selected.collect(), expected)
    destination = tmp_path / "saved.parquet"
    selected.save(destination)
    table.close()
    assert not Path(directory).exists()
    assert_frame_equal(pl.read_parquet(destination), expected)
    with pytest.raises(RuntimeError, match="closed"):
        selected.collect()
    table.close()


@pytest.mark.parametrize("error", [OSError("write failed"), KeyboardInterrupt()])
def test_interrupted_result_save_preserves_existing_file(tmp_path, monkeypatch, error):
    table = ResultTable()
    table._append(pl.DataFrame({"value": [1, 2]}))
    destination = tmp_path / "export.parquet"
    original = pl.DataFrame({"value": [9]})
    original.write_parquet(destination)

    def fail_after_partial_write(frame, path, *args, **kwargs):
        Path(path).write_bytes(b"partial")
        raise error

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", fail_after_partial_write)
    with pytest.raises(type(error)):
        table.save(destination)
    assert_frame_equal(pl.read_parquet(destination), original)
    assert not list(tmp_path.glob(".export.parquet.*.tmp"))
    assert_frame_equal(table.collect(), pl.DataFrame({"value": [1, 2]}))
    table.close()


def test_result_table_context_cleans_up_after_failure():
    with pytest.raises(ValueError, match="failure"):
        with ResultTable() as table:
            directory = table._directory.name
            table._append(pl.DataFrame({"value": [1]}))
            raise ValueError("failure")
    assert not Path(directory).exists()


def test_result_table_lives_until_its_last_view_is_released():
    table = ResultTable()
    directory = Path(table._directory.name)
    table._append(pl.DataFrame({"value": [1, 2]}))
    view = table.filter(pl.col("value") == 2)
    del table
    assert directory.exists()
    assert view.collect()["value"].to_list() == [2]
    del view
    assert not directory.exists()


def test_grouped_results_keep_files_until_the_aggregate_is_closed():
    table = ResultTable()
    directory = Path(table._directory.name)
    table._append(pl.DataFrame({"mode": ["car", "walk"], "people": [2, 1]}))
    table._append(pl.DataFrame({"mode": ["car"], "people": [3]}))
    grouped = table.filter(pl.col("people") > 1).group_by("mode")
    del table
    assert directory.exists()
    result = grouped.agg(trips=pl.col("people").sum()).sort("mode")
    del grouped
    assert_frame_equal(result.collect(engine="streaming"), pl.DataFrame({"mode": ["car"], "trips": [5]}))
    result.close()
    assert not directory.exists()
    with pytest.raises(RuntimeError, match="closed"):
        result.collect()


def test_temporary_limit_counts_open_results_and_removes_failed_output(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "1000000")
    first = ResultTable()
    first._append(pl.DataFrame({"value": list(range(100))}).lazy())
    used = managed_temporary_disk_usage()
    assert used > 0
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", str(used))
    second = ResultTable()
    with pytest.raises(TemporaryDiskLimitExceeded, match="temporary files reached"):
        second._append(pl.DataFrame({"value": [1]}).lazy())
    assert not list(Path(second._directory.name).iterdir())
    assert first.collect().height == 100
    first.close()
    assert managed_temporary_disk_usage() == 0
    second._append(pl.DataFrame({"value": [1]}).lazy())
    assert second.collect()["value"].to_list() == [1]
    second.close()


def test_temporary_limit_applies_to_pandas_parquet(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    folder = tmp_path / "temporary" / "mobility-run-assets-test"
    folder.mkdir(parents=True)
    path = folder / "flows.parquet"
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "100000")
    frame = pd.DataFrame({"value": [1, 2]})
    with temporary_file_writer(path) as file:
        frame.to_parquet(file, index=False)
    assert pd.read_parquet(path).equals(frame)
    path.unlink()
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "1")
    with pytest.raises(TemporaryDiskLimitExceeded):
        with temporary_file_writer(path) as file:
            frame.to_parquet(file, index=False)
    assert not path.exists()


def test_external_temporary_write_is_checked_after_handoff(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "10")
    folder = tmp_path / "temporary" / "mobility-run-assets-test"
    folder.mkdir(parents=True)
    (folder / "external-output").write_bytes(b"12345678901")
    with pytest.raises(TemporaryDiskLimitExceeded, match="exceeding"):
        check_temporary_disk_limit()


@pytest.mark.parametrize("error", [OSError("disk error"), KeyboardInterrupt()])
def test_failed_unlimited_spill_does_not_leave_partial_file(tmp_path, monkeypatch, error):
    monkeypatch.delenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", raising=False)
    path = tmp_path / "spill.parquet"

    class FailedWrite:
        def write_parquet(self, destination):
            destination.write_bytes(b"partial")
            raise error

    with pytest.raises(type(error)):
        write_temporary_parquet(FailedWrite(), path)
    assert not path.exists()


@pytest.mark.parametrize(
    ("asset_class", "module"),
    [
        (DestinationSequences, "mobility.trips.group_day_trips.plans.destination_sequences"),
        (ModeSequences, "mobility.trips.group_day_trips.plans.mode_sequence_search.mode_sequences"),
    ],
)
def test_sequence_spills_obey_limit_and_publish_only_complete_files(tmp_path, monkeypatch, asset_class, module):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    folder = tmp_path / "temporary" / "mobility-shared-assets-test"
    folder.mkdir(parents=True)
    asset = object.__new__(asset_class)
    sequences = pl.DataFrame({"value": list(range(100))})
    asset.sequence_index = pl.DataFrame({"id": list(range(100))})
    if asset_class is ModeSequences:
        asset.generated_mode_sequences = 17
    sample = tmp_path / "sample.parquet"
    sequences.write_parquet(sample)
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", str(sample.stat().st_size + 64))
    path = folder / "sequences"
    with pytest.raises(TemporaryDiskLimitExceeded):
        asset._spill_sequences(sequences, path)
    assert not path.exists()
    assert not list(folder.iterdir())
    assert managed_temporary_disk_usage() == 0

    monkeypatch.delenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES")
    asset._spill_sequences(sequences, path)
    assert_frame_equal(asset._load_spilled_sequences(path), sequences)
    assert_frame_equal(asset.sequence_index, pl.DataFrame({"id": list(range(100))}))
    if asset_class is ModeSequences:
        assert asset.generated_mode_sequences == 17
    existing = (path / "sequences.parquet").read_bytes()
    with pytest.raises(FileExistsError):
        asset._spill_sequences(sequences, path)
    assert (path / "sequences.parquet").read_bytes() == existing

    original_write = write_temporary_parquet

    def interrupt_index(frame, destination):
        if destination.name == "sequence_index.parquet":
            raise KeyboardInterrupt()
        return original_write(frame, destination)

    monkeypatch.setattr(f"{module}.write_temporary_parquet", interrupt_index)
    interrupted_path = folder / "interrupted"
    with pytest.raises(KeyboardInterrupt):
        asset._spill_sequences(sequences, interrupted_path)
    assert not interrupted_path.exists()
    assert list(folder.iterdir()) == [path]


@pytest.mark.parametrize("asset_class", [DestinationSequences, ModeSequences])
def test_shared_sequence_spill_limit_fails_without_leaving_a_partial_asset(tmp_path, monkeypatch, asset_class):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "1")
    asset = object.__new__(asset_class)
    InMemoryAsset.__init__(asset, {"test": "sequence-spill"})
    asset.sequence_index = pl.DataFrame({"id": [1]})
    if asset_class is ModeSequences:
        asset.generated_mode_sequences = 1

    with asset_resolution_context() as resolver:
        key = asset_graph_key(asset)
        resolver.shared_asset_keys.add(key)
        with pytest.raises(TemporaryDiskLimitExceeded):
            resolver.get_in_memory_asset(
                asset, lambda: pl.DataFrame({"value": [1]}),
                spill_to_disk=asset._spill_sequences,
                load_from_disk=asset._load_spilled_sequences,
            )
        assert key not in resolver.in_memory_values
        assert not list(Path(resolver._spill_directory.name).iterdir())

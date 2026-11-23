"""Cross-feed stop and time matching must not depend on GTFS identifiers."""

import datetime as dt

import pandas as pd
import pytest

from mobility.transport.modes.public_transport.gtfs.gtfs_overlap import find_trip_overlaps, find_within_feed_duplicates
from mobility.transport.modes.public_transport.gtfs.gtfs_timetable import GTFSTimetable


def tables():
    trips = pd.DataFrame({"trip_id": ["1-first", "2-other", "2-late", "2-headway"],
                          "_frequency_kind": ["scheduled", "exact", "scheduled", "headway"]})
    stops = pd.DataFrame({"stop_id": ["1-a", "1-b", "2-different-a", "2-different-b"],
                          "stop_lon": [4.03, 4.04, 4.0301, 4.0401],
                          "stop_lat": [49.25, 49.26, 49.2501, 49.2601]})
    rows = []
    for trip, stop_ids, shift in [
        ("1-first", ["1-a", "1-b"], 0),
        ("2-other", ["2-different-a", "2-different-b"], 30),
        ("2-late", ["2-different-a", "2-different-b"], 600),
        ("2-headway", ["2-different-a", "2-different-b"], 30),
    ]:
        for sequence, stop in enumerate(stop_ids, 1):
            rows.append((trip, stop, sequence, 1000 + sequence * 300 + shift,
                         1010 + sequence * 300 + shift))
    times = pd.DataFrame(rows, columns=["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"])
    times["pickup_type"] = 0
    times["drop_off_type"] = 0
    return trips, times, stops


def test_matches_different_ids_without_matching_late_or_headway():
    trips, times, stops = tables()
    result = find_trip_overlaps(trips, times, stops, distance_m=100, time_seconds=120, batch_size=1)
    assert result.select("left_trip_id", "right_trip_id").rows() == [("1-first", "2-other")]
    assert result["max_distance_m"][0] < 20
    assert result["max_time_difference_s"][0] == 30


def test_default_matching_requires_same_times_and_stops_within_ten_metres():
    trips, times, stops = tables()
    stops.loc[stops.stop_id == "2-different-a", ["stop_lon", "stop_lat"]] = [4.03, 49.25]
    stops.loc[stops.stop_id == "2-different-b", ["stop_lon", "stop_lat"]] = [4.04, 49.26]
    assert find_trip_overlaps(trips, times, stops).is_empty()
    selected = times.trip_id == "2-other"
    times.loc[selected, ["arrival_time", "departure_time"]] -= 30
    assert find_trip_overlaps(trips, times, stops).height == 1


def test_reverse_order_and_changed_intermediate_time_do_not_match():
    trips, times, stops = tables()
    times.loc[times.trip_id == "2-other", "stop_sequence"] = [2, 1]
    assert find_trip_overlaps(trips, times, stops, distance_m=100, time_seconds=120).is_empty()


@pytest.mark.parametrize("rule", ["pickup_type", "drop_off_type"])
def test_different_boarding_rules_are_not_matches(rule):
    trips, times, stops = tables()
    times.loc[(times.trip_id == "2-other") & (times.stop_sequence == 2), rule] = 1
    assert find_trip_overlaps(trips, times, stops, distance_m=100, time_seconds=120).is_empty()


def test_invalid_thresholds_fail():
    trips, times, stops = tables()
    with pytest.raises(ValueError, match="positive"):
        find_trip_overlaps(trips, times, stops, distance_m=0)


def test_priority_uses_one_to_one_matching_for_ambiguous_pairs():
    timetable = GTFSTimetable(["a", "b"], None, "unused", feed_priority=["a", "b"])
    pairs = pd.DataFrame({"left_trip_id": ["1-a", "1-a", "1-b"],
                          "right_trip_id": ["2-a", "2-b", "2-a"],
                          "left_source": ["1"] * 3, "right_source": ["2"] * 3,
                          "max_time_difference_s": [0, 0, 0], "max_distance_m": [0, 0, 0]})
    identities = pd.DataFrame({"trip_id": ["1-a", "1-b", "2-a", "2-b"],
                               "agency_name": ["A", "A", "B", "B"]})
    sources = [{"feed_id": "1", "path": "a"}, {"feed_id": "2", "path": "b"}]
    removed = timetable.resolve_trip_overlaps(pairs, identities, sources, pd.Timestamp(dt.date(2026, 10, 6)))
    assert removed == {"2-a", "2-b"}
    assert pairs.selected_for_removal.sum() == 2


def test_priority_prefers_exact_times_among_equally_complete_matchings(caplog):
    timetable = GTFSTimetable(["a", "b"], None, "unused", feed_priority=["a", "b"])
    pairs = pd.DataFrame({"left_trip_id": ["1-a", "1-a", "1-b", "1-b"],
                          "right_trip_id": ["2-a", "2-b", "2-a", "2-b"],
                          "left_source": ["1"] * 4, "right_source": ["2"] * 4,
                          "max_time_difference_s": [60, 0, 0, 60], "max_distance_m": [0] * 4})
    identities = pd.DataFrame({"trip_id": ["1-a", "1-b", "2-a", "2-b"],
                               "agency_name": ["A", "A", "B", "B"]})
    sources = [{"feed_id": "1", "path": "a"}, {"feed_id": "2", "path": "b"}]
    timetable.resolve_trip_overlaps(pairs, identities, sources, pd.Timestamp(dt.date(2026, 10, 6)))
    selected = pairs.loc[pairs.selected_for_removal, ["left_trip_id", "right_trip_id"]]
    assert set(map(tuple, selected.to_numpy())) == {("1-a", "2-b"), ("1-b", "2-a")}
    assert "multiple candidate matches" in caplog.text


def test_five_matching_feeds_keep_only_highest_priority_trip():
    feeds = ["a", "b", "c", "d", "e"]
    timetable = GTFSTimetable(feeds, None, "unused", feed_priority=feeds)
    rows = [(str(i), str(j), f"{i}-trip", f"{j}-trip")
            for i in range(1, 6) for j in range(i + 1, 6)]
    pairs = pd.DataFrame(rows, columns=["left_source", "right_source", "left_trip_id", "right_trip_id"])
    pairs["max_time_difference_s"] = 0
    pairs["max_distance_m"] = 0
    identities = pd.DataFrame({"trip_id": [f"{i}-trip" for i in range(1, 6)], "agency_name": feeds})
    sources = [{"feed_id": str(i), "path": feed} for i, feed in enumerate(feeds, 1)]
    removed = timetable.resolve_trip_overlaps(pairs, identities, sources, pd.Timestamp(dt.date(2026, 10, 6)))
    assert removed == {f"{i}-trip" for i in range(2, 6)}
    assert pairs.selected_for_removal.sum() == 4


def test_matching_chain_does_not_remove_trip_without_retained_match():
    timetable = GTFSTimetable(["a", "b", "c"], None, "unused", feed_priority=["a", "b", "c"])
    pairs = pd.DataFrame({"left_source": ["1", "2"], "right_source": ["2", "3"],
                          "left_trip_id": ["1-a", "2-b"], "right_trip_id": ["2-b", "3-c"],
                          "max_time_difference_s": [0, 0], "max_distance_m": [0, 0]})
    identities = pd.DataFrame({"trip_id": ["1-a", "2-b", "3-c"], "agency_name": ["A", "B", "C"]})
    sources = [{"feed_id": str(i), "path": feed} for i, feed in enumerate(["a", "b", "c"], 1)]
    removed = timetable.resolve_trip_overlaps(pairs, identities, sources, pd.Timestamp(dt.date(2026, 10, 6)))
    assert removed == {"2-b"}


def test_within_feed_requires_same_route_times_and_boarding_rules():
    trips = pd.DataFrame({"trip_id": ["1-a", "1-b", "1-c", "1-d", "1-e"],
                          "route_id": ["1-r", "1-r", "1-r", "1-other", "1-r"],
                          "_frequency_kind": ["scheduled", "exact", "scheduled", "scheduled", "headway"]})
    times = pd.DataFrame([(trip, stop, sequence, time, time, int(trip == "1-c"), 0)
                          for trip in trips.trip_id
                          for sequence, stop, time in [(1, "s1", 100), (2, "s2", 200)]],
                         columns=["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "pickup_type", "drop_off_type"])
    duplicates = find_within_feed_duplicates(trips, times)
    assert duplicates.select("source", "kept_trip_id", "removed_trip_id").rows() == [("1", "1-a", "1-b")]


def test_five_identical_trips_in_one_feed_keep_one():
    trip_ids = [f"1-{i}" for i in range(5)]
    trips = pd.DataFrame({"trip_id": trip_ids, "route_id": ["1-route"] * 5,
                          "_frequency_kind": ["scheduled"] * 5})
    times = pd.DataFrame([(trip, stop, sequence, time, time, 0, 0)
                          for trip in trip_ids for sequence, stop, time in [(1, "a", 100), (2, "b", 200)]],
                         columns=["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "pickup_type", "drop_off_type"])
    duplicates = find_within_feed_duplicates(trips, times)
    assert duplicates["kept_trip_id"].to_list() == ["1-0"] * 4
    assert set(duplicates["removed_trip_id"]) == set(trip_ids[1:])

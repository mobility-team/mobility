"""Correct activities recorded with zero duration in mobility surveys.

An activity duration is the time between one trip's arrival and the next
trip's departure:

    incoming trip arrives -> activity -> outgoing trip departs

When both clocks show 09:00, the survey reports a zero-duration activity even
though the person probably stopped briefly. This module fits plausible short
durations from the same survey and moves the two clocks apart. The activity
after the last trip is not corrected because its end is outside the diary.

The timing resolution is inferred from each diary. If every clock lies on a
five-minute boundary, reported durations use a conservative five-minute
interval half-width. Other diaries use a one-minute half-width.
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import gammainc


_MINIMUM_MOTIVE_OBSERVATIONS = 30
_MINIMUM_PROBABILITY = np.finfo(np.float64).tiny
_MISSING_MOTIVE = "__missing__"


def _gamma_cdf(
    upper: np.ndarray | float,
    shape: float,
    scale: float,
) -> np.ndarray:
    """Return the Gamma cumulative probability through ``upper``."""
    return gammainc(shape, np.asarray(upper, dtype=np.float64) / scale)


def _interval_probability(
    observed: np.ndarray,
    interval_half_width: np.ndarray,
    shape: float,
    scale: float,
) -> np.ndarray:
    """Return Gamma mass around each reported duration."""
    lower = np.maximum(0.0, observed - interval_half_width)
    upper = observed + interval_half_width
    probability = _gamma_cdf(upper, shape, scale) - _gamma_cdf(
        lower,
        shape,
        scale,
    )
    return np.maximum(probability, _MINIMUM_PROBABILITY)


def _fit_gamma(observations: pd.DataFrame) -> tuple[float, float] | None:
    """Fit one interval-censored Gamma distribution."""
    grouped = (
        observations.groupby(
            ["observed_minutes", "interval_half_width_minutes"],
            as_index=False,
            sort=True,
        )["weight"]
        .sum()
    )
    observed = grouped["observed_minutes"].to_numpy()
    half_width = grouped["interval_half_width_minutes"].to_numpy()
    weights = grouped["weight"].to_numpy()
    weights = weights / weights.sum()

    # Use approximate unrounded durations for a stable initial estimate only.
    initial_durations = np.maximum(observed, half_width * 0.5)
    mean = max(float(np.average(initial_durations, weights=weights)), 0.1)
    variance = max(
        float(
            np.average(
                np.square(initial_durations - mean),
                weights=weights,
            )
        ),
        mean,
    )
    initial_shape = min(max(mean * mean / variance, 0.05), 20.0)
    initial_scale = min(max(variance / mean, 0.05), 24.0 * 60.0)

    def negative_log_likelihood(log_parameters: np.ndarray) -> float:
        shape, scale = np.exp(log_parameters)
        probability = _interval_probability(
            observed,
            half_width,
            float(shape),
            float(scale),
        )
        return float(-np.dot(weights, np.log(probability)))

    result = minimize(
        negative_log_likelihood,
        np.log([initial_shape, initial_scale]),
        method="L-BFGS-B",
        bounds=[(-7.0, 7.0), (-5.0, math.log(24.0 * 60.0))],
    )
    if not result.success or not np.isfinite(result.fun):
        return None
    shape, scale = np.exp(result.x)
    return float(shape), float(scale)


def _zero_interval_mean(
    shape: float,
    scale: float,
    interval_half_width: int,
) -> float:
    """Return mean duration given a reported value of zero."""
    interval_probability = float(
        _gamma_cdf(interval_half_width, shape, scale)
    )
    first_moment = (
        shape
        * scale
        * float(
            gammainc(
                shape + 1.0,
                interval_half_width / scale,
            )
        )
    )
    return first_moment / interval_probability


def _prepare_activity_durations(
    trips: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Index] | None:
    """Sort diaries and derive activity durations and timing resolution."""
    required_columns = {
        "individual_id",
        "daily_trip_index",
        "departure_time",
        "arrival_time",
        "motive",
    }
    if not required_columns.issubset(trips.columns):
        return None

    prepared = trips.copy()
    original_index = prepared.index.copy()
    prepared["_original_order"] = np.arange(len(prepared))
    if "day_id" in prepared.columns:
        prepared["_diary_day_id"] = prepared["day_id"].to_numpy()
    elif "day_id" in prepared.index.names:
        prepared["_diary_day_id"] = prepared.index.get_level_values("day_id")
    else:
        return None

    prepared.reset_index(drop=True, inplace=True)
    diary_columns = ["_diary_day_id", "individual_id"]
    prepared.sort_values(
        diary_columns + ["daily_trip_index"],
        kind="stable",
        inplace=True,
    )
    diary = [prepared[column] for column in diary_columns]

    prepared["_departure"] = pd.to_numeric(
        prepared["departure_time"],
        errors="coerce",
    )
    prepared["_arrival"] = pd.to_numeric(
        prepared["arrival_time"],
        errors="coerce",
    )

    def lies_on_five_minute_boundary(seconds: pd.Series) -> np.ndarray:
        distance = np.mod(seconds, 300.0)
        return np.isclose(distance, 0.0, atol=0.5) | np.isclose(
            distance,
            300.0,
            atol=0.5,
        )

    clocks_on_five_minutes = pd.Series(
        lies_on_five_minute_boundary(prepared["_departure"])
        & lies_on_five_minute_boundary(prepared["_arrival"]),
        index=prepared.index,
    )
    prepared["_interval_half_width_minutes"] = np.where(
        clocks_on_five_minutes.groupby(diary).transform("all"),
        5,
        1,
    )
    prepared["_next_departure"] = prepared["_departure"].groupby(diary).shift(
        -1
    )
    prepared["_observed_minutes"] = (
        (prepared["_next_departure"] - prepared["_arrival"])
        / 60.0
    )
    prepared["_zero_duration"] = prepared["_next_departure"].notna() & (
        np.isclose(
            prepared["_next_departure"] - prepared["_arrival"],
            0.0,
            atol=0.5,
        )
    )
    return prepared, original_index


def _fit_five_minute_corrections(
    prepared: pd.DataFrame,
) -> dict[str, int]:
    """Fit corrections only for motives containing reported zeros.

    A motive with fewer than 30 observations uses the survey-wide fit.
    """
    weights = (
        prepared["pondki"]
        if "pondki" in prepared.columns
        else pd.Series(1.0, index=prepared.index)
    )
    observations = pd.DataFrame(
        {
            "motive": prepared["motive"]
            .astype("string")
            .fillna(_MISSING_MOTIVE),
            "observed_minutes": prepared["_observed_minutes"],
            "interval_half_width_minutes": prepared[
                "_interval_half_width_minutes"
            ],
            "weight": pd.to_numeric(weights, errors="coerce"),
        }
    )
    observations = observations.loc[
        observations["observed_minutes"].between(0.0, 24.0 * 60.0)
        & observations["weight"].gt(0.0)
        & observations["weight"].notna()
    ]
    if len(observations) < _MINIMUM_MOTIVE_OBSERVATIONS:
        return {}

    pooled_fit = _fit_gamma(observations)
    zero_motives = (
        prepared.loc[prepared["_zero_duration"], "motive"]
        .astype("string")
        .fillna(_MISSING_MOTIVE)
        .unique()
    )
    corrections: dict[str, int] = {}
    for motive in zero_motives:
        motive_observations = observations.loc[
            observations["motive"] == motive
        ]
        parameters = pooled_fit
        if len(motive_observations) >= _MINIMUM_MOTIVE_OBSERVATIONS:
            parameters = _fit_gamma(motive_observations) or pooled_fit
        if parameters is None:
            continue
        shape, scale = parameters
        corrections[str(motive)] = math.ceil(
            _zero_interval_mean(shape, scale, 5)
        )
    return corrections


def _apply_clock_corrections(
    prepared: pd.DataFrame,
    five_minute_corrections: dict[str, int],
) -> None:
    """Move the two trip clocks around each zero-duration activity.

    For a four-minute correction around 09:00, the preferred split moves the
    incoming arrival to 08:58 and the outgoing departure to 09:02. If either
    trip is shorter than two minutes, more of the correction is assigned to
    the other trip so travel durations cannot become negative.
    """
    motives = prepared["motive"].astype("string").fillna(_MISSING_MOTIVE)
    correction_minutes = np.where(
        prepared["_interval_half_width_minutes"] == 1,
        1,
        motives.map(five_minute_corrections).fillna(0),
    )
    requested_correction = np.where(
        prepared["_zero_duration"],
        correction_minutes * 60.0,
        0.0,
    )

    remaining_travel_time = np.nan_to_num(
        (prepared["_arrival"] - prepared["_departure"]).to_numpy(),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    remaining_travel_time = np.maximum(remaining_travel_time, 0.0)
    arrival_shift = np.zeros(len(prepared))
    departure_shift = np.zeros(len(prepared))
    was_corrected = np.zeros(len(prepared), dtype=bool)

    for incoming in np.flatnonzero(requested_correction):
        # Rows are sorted by diary and trip index. A requested correction
        # therefore always belongs to the trip immediately after this one.
        outgoing = incoming + 1
        requested = requested_correction[incoming]
        feasible = min(
            requested,
            remaining_travel_time[incoming]
            + remaining_travel_time[outgoing],
        )
        incoming_share = np.clip(
            feasible * 0.5,
            feasible - remaining_travel_time[outgoing],
            remaining_travel_time[incoming],
        )
        outgoing_share = feasible - incoming_share

        arrival_shift[incoming] = incoming_share
        departure_shift[outgoing] = outgoing_share
        remaining_travel_time[incoming] -= incoming_share
        remaining_travel_time[outgoing] -= outgoing_share
        was_corrected[incoming] = feasible > 0.0

    prepared["arrival_time"] = (
        prepared["_arrival"].to_numpy() - arrival_shift
    )
    prepared["departure_time"] = (
        prepared["_departure"].to_numpy() + departure_shift
    )
    prepared["_was_corrected"] = was_corrected


def _correct_zero_duration_activities(
    trips: pd.DataFrame,
) -> pd.DataFrame:
    """Return survey trips with plausible durations for reported-zero stays."""
    result = _prepare_activity_durations(trips)
    if result is None:
        return trips
    prepared, original_index = result
    if not prepared["_zero_duration"].any():
        return trips

    corrections = _fit_five_minute_corrections(prepared)
    _apply_clock_corrections(
        prepared,
        corrections,
    )
    correction_count = int(prepared["_was_corrected"].sum())
    if correction_count == 0:
        return trips

    prepared.sort_values("_original_order", kind="stable", inplace=True)
    output_columns = list(trips.columns)
    corrected = prepared[output_columns].copy()
    corrected.index = original_index
    logging.info(
        "Corrected %s zero-duration activities between consecutive trips",
        correction_count,
    )
    return corrected

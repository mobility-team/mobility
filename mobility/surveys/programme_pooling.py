"""Borrow complete EMP days without changing their survey probabilities."""

import numpy as np
import polars as pl
from pydantic import BaseModel, ConfigDict, Field
from scipy.optimize import minimize
from scipy.special import logsumexp


class ProgrammePoolingParameters(BaseModel):
    """Control borrowing; these settings never enter the reference calculation."""

    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    pooling_strength: float = Field(
        default=30.0, gt=0, allow_inf_nan=False,
        description="Effective sample size giving equal local and broader weight. Higher values borrow more.",
    )
    fit_regularization: float = Field(
        default=0.1, gt=0, allow_inf_nan=False,
        description="Limits fitted weight adjustments. Higher values keep probabilities closer to starting weights.",
    )


def build_donors(steps: pl.DataFrame, survey_name: str) -> pl.DataFrame:
    """Keep one row per respondent day, including its unchanged reference mass."""
    segments = ["is_weekday", "city_category", "csp", "n_cars"]
    return (
        steps.group_by(["individual_id", "day_id", *segments])
        .agg(
            survey_weight=pl.col("pondki").first(),
            activity_seq_id=pl.col("activity_seq_id").first(),
            time_seq_id=pl.col("time_seq_id").first(),
            activities=pl.col("activity").sort_by("seq_step_index"),
            departures=pl.col("departure_time").sort_by("seq_step_index"),
            arrivals=pl.col("arrival_time").sort_by("seq_step_index"),
            next_departures=pl.col("next_departure_time").sort_by("seq_step_index"),
            durations=pl.col("duration_per_pers").sort_by("seq_step_index"),
        )
        .filter(pl.col("survey_weight").is_finite() & (pl.col("survey_weight") > 0))
        .with_columns(
            donor_day_id=pl.concat_str([
                pl.lit(survey_name), pl.col("individual_id"), pl.col("day_id")
            ], separator=":"),
            source_segment=pl.struct(segments),
            p_reference=pl.col("survey_weight") / pl.col("survey_weight").sum().over(segments),
        )
        .sort("donor_day_id")
    )


def _programme_features(donors: pl.DataFrame, activities: list[str] | None = None) -> tuple[np.ndarray, list[str]]:
    """Count trips, destinations, departure bins and duration bins, never modes."""
    if activities is None:
        activities = sorted({a for row in donors["activities"] for a in row})
    names = ["trip_count"] + [f"activity:{a}" for a in activities]
    names += [f"departure:{hour}-{hour + 4}" for hour in range(0, 24, 4)]
    names += ["duration:0-1", "duration:1-3", "duration:3-6", "duration:6+"]
    features = []
    for row in donors.iter_rows(named=True):
        counts = [len(row["activities"])]
        counts += [row["activities"].count(a) for a in activities]
        counts += np.histogram(row["departures"], bins=np.arange(0, 25, 4))[0].tolist()
        # The last home stay ends at the diary boundary, not at a next departure.
        counts += np.histogram(row["durations"][:-1], bins=[0, 1, 3, 6, np.inf])[0].tolist()
        features.append(counts)
    return np.asarray(features, dtype=float).reshape(len(donors), len(names)), names


def _fit_programme_probabilities(features, target_weights, log_starting_weights, regularization):
    """Adjust donor weights toward target summaries while limiting large changes."""
    # Put summaries on similar scales, so large counts do not dominate the fit.
    features = features / np.maximum(np.std(features, axis=0), 1.0)
    target_means = target_weights @ features
    log_starting_weights = log_starting_weights - logsumexp(log_starting_weights)

    def objective(coefficients):
        # Each coefficient raises or lowers weights for one programme summary.
        log_weights = log_starting_weights + features @ coefficients
        normalizer = logsumexp(log_weights)
        probabilities = np.exp(log_weights - normalizer)
        loss = normalizer - target_means @ coefficients + regularization * (coefficients @ coefficients) / 2
        gradient = probabilities @ features - target_means + regularization * coefficients
        return loss, gradient

    fit = minimize(objective, np.zeros(features.shape[1]), jac=True, method="L-BFGS-B")
    if not fit.success:
        raise RuntimeError(f"Programme probability fit failed: {fit.message}")
    log_weights = log_starting_weights + features @ fit.x
    return np.exp(log_weights - logsumexp(log_weights))


def pool_programmes(
    donors: pl.DataFrame,
    targets: pl.DataFrame,
    settings: ProgrammePoolingParameters,
) -> pl.DataFrame:
    """Fit one assignment per compatible target segment and donor day.

    Kish effective sample size controls successive shrinkage from the national
    compatible group to occupation, occupation/urban category, then exact segment.
    A regularized entropy fit adjusts survey-weighted donor probabilities toward
    those smoothed programme summaries. It cannot alter reference probabilities.
    With pooling disabled, return the eligible-day baseline for held-out
    validation. Actual disabled model runs keep their legacy survey plans.
    """
    keys = ["is_weekday", "city_category", "csp", "n_cars"]
    preferences = ["csp", "city_category", "n_cars"]
    adult_categories = ["fr-1", "fr-2", "fr-3", "fr-4", "fr-5", "fr-6", "fr-7", "fr-8b"]
    if settings.enabled:
        features, _ = _programme_features(donors)
    rows = []
    for target in targets.select(keys).unique().sort(keys).iter_rows(named=True):
        # EMP explicitly separates under-15 inactive people (8a). No finer age
        # group is represented in current demand segments. Unknown CSPs borrow
        # only within their own CSP; adults 1-8b may borrow across occupations.
        compatible = (
            (donors["is_weekday"] == target["is_weekday"])
            & (donors["csp"].is_in(adult_categories) if target["csp"] in adult_categories
               else donors["csp"] == target["csp"])
        ).to_numpy()
        indices = np.flatnonzero(compatible)
        if not len(indices):
            continue
        candidates = donors[indices.tolist()]
        matches = np.column_stack([(candidates[key] == target[key]).to_numpy() for key in preferences])
        reference_probability = np.where(matches.all(axis=1), candidates["p_reference"].to_numpy(), 0.0)
        probability = reference_probability.copy()
        if settings.enabled:
            survey_weights = candidates["survey_weight"].to_numpy()
            national = survey_weights / survey_weights.sum()
            smoothed = national.copy()
            # Smooth targets from national to occupation, then urban category,
            # then cars. The final local_share belongs to the exact segment.
            for level in range(len(preferences)):
                local_weights = survey_weights * matches[:, :level + 1].all(axis=1)
                local_share = 0.0
                if local_weights.sum() > 0:
                    effective_days = local_weights.sum() ** 2 / np.square(local_weights).sum()
                    local_share = effective_days / (effective_days + settings.pooling_strength)
                    smoothed = local_share * local_weights / local_weights.sum() + (1 - local_share) * smoothed

            # Each mismatched preference reduces starting weight by exp(-1).
            log_starting_weights = np.log(national) - (~matches).sum(axis=1)
            fitted = _fit_programme_probabilities(
                features[indices], smoothed, log_starting_weights, settings.fit_regularization,
            )
            # Retain exact programmes as evidence grows, even when the fitted
            # summaries cannot distinguish two different schedules.
            probability = local_share * reference_probability + (1 - local_share) * fitted

        rows.append(candidates.with_columns(
            *[pl.lit(target[key]).cast(donors.schema[key]).alias(key) for key in keys],
            p_sampling=pl.Series(probability),
            p_reference=pl.Series(reference_probability),
        ))
    if not rows:
        return donors.head(0).with_columns(p_sampling=pl.lit(0.0))
    return pl.concat(rows)


def pooling_diagnostics(assignments: pl.DataFrame) -> pl.DataFrame:
    """Report segment coverage, observed support, borrowing and donor reuse."""
    keys = ["is_weekday", "city_category", "csp", "n_cars"]
    reuse = assignments.filter(pl.col("p_sampling") > 0).group_by("donor_day_id").len(name="donor_reuse")
    return (
        assignments.join(reuse, on="donor_day_id", how="left")
        .group_by(keys).agg(
            sampling_mass=pl.col("p_sampling").sum(),
            observed_days=(pl.col("p_reference") > 0).sum(),
            available_donors=(pl.col("p_sampling") > 0).sum(),
            effective_sample_size=pl.when(pl.col("p_sampling").sum() > 0).then(
                pl.col("p_sampling").sum().pow(2) / pl.col("p_sampling").pow(2).sum()
            ).otherwise(None),
            reference_effective_sample_size=pl.when(pl.col("p_reference").sum() > 0).then(
                pl.col("p_reference").sum().pow(2) / pl.col("p_reference").pow(2).sum()
            ).otherwise(None),
            borrowed_mass=pl.when(pl.col("p_reference") == 0).then(pl.col("p_sampling")).otherwise(0).sum(),
            mean_donor_reuse=(pl.col("p_sampling") * pl.col("donor_reuse").fill_null(0)).sum(),
        ).sort(keys)
    )


def validate_pooling(
    steps: pl.DataFrame, survey_name: str, settings: ProgrammePoolingParameters,
    *, seed: int = 0, test_share: float = 0.2,
) -> pl.DataFrame:
    """Compare fitted programme summaries with held-out respondent days.

    Split respondents, not schedules or steps, before fitting. Return weighted
    held-out means and predictions for each covered segment and feature. The
    original reference used by Mobility is not replaced by this validation set.
    """
    if not 0 < test_share < 1:
        raise ValueError("test_share must be between zero and one")
    respondents = steps["individual_id"].unique().sort().to_numpy()
    rng = np.random.default_rng(seed)
    test_ids = respondents[rng.random(len(respondents)) < test_share].tolist()
    training = steps.filter(~pl.col("individual_id").is_in(test_ids))
    testing = steps.filter(pl.col("individual_id").is_in(test_ids))
    if training.is_empty() or testing.is_empty():
        raise ValueError("The split needs both training and held-out respondents")
    donors = build_donors(training, survey_name)
    held_out = build_donors(testing, survey_name)
    keys = ["is_weekday", "city_category", "csp", "n_cars"]
    assignments = pool_programmes(donors, held_out.select(keys).unique(), settings)
    activities = sorted({a for row in donors["activities"] for a in row}
                        | {a for row in held_out["activities"] for a in row})
    train_x, names = _programme_features(donors, activities)
    test_x, _ = _programme_features(held_out, activities)
    train_features = pl.DataFrame(train_x, schema=names).with_columns(donors["donor_day_id"])
    test_features = pl.DataFrame(test_x, schema=names).with_columns(held_out["donor_day_id"])
    predicted = assignments.join(train_features, on="donor_day_id").group_by(keys).agg(
        *[(pl.col(name) * pl.col("p_sampling")).sum().alias(name) for name in names],
        covered=pl.col("p_sampling").sum() > 0,
    ).unpivot(on=names, index=[*keys, "covered"], variable_name="feature", value_name="prediction")
    observed = held_out.join(test_features, on="donor_day_id").group_by(keys).agg(
        *[(pl.col(name) * pl.col("p_reference")).sum().alias(name) for name in names],
        held_out_days=pl.len(),
    ).unpivot(on=names, index=[*keys, "held_out_days"], variable_name="feature", value_name="held_out_mean")
    return observed.join(predicted, on=[*keys, "feature"], how="left").with_columns(
        covered=pl.col("covered").fill_null(False),
        prediction=pl.when(pl.col("covered")).then(pl.col("prediction")).otherwise(None),
    ).with_columns(
        absolute_error=(pl.col("prediction") - pl.col("held_out_mean")).abs(),
    ).sort([*keys, "feature"])

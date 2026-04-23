from __future__ import annotations

import logging
from time import perf_counter

import polars as pl


class PlanDistance:
    """Compute transition distances from direct hourly plan features."""
    spatial_scale_meters: float = 100_000.0

    def get_plan_pair_distances(
        self,
        pair_index: pl.DataFrame,
        plan_steps: pl.DataFrame,
        *,
        plan_id_col: str = "plan_id",
        transport_zones=None,
        dimension_weights: list[float] | None = None,
    ) -> pl.DataFrame:
        """Return distances only for the requested plan pairs."""
        t0 = perf_counter()
        if pair_index.height == 0:
            return pair_index.with_columns(distance=pl.lit(0.0, dtype=pl.Float64))

        requested_plan_ids = pl.concat(
            [
                pair_index.select(pl.col("plan_id_from").alias(plan_id_col)),
                pair_index.select(pl.col("plan_id_trans").alias(plan_id_col)),
            ],
            how="vertical_relaxed",
        ).unique()
        logging.info(
            "PlanDistance: start pair_count=%s requested_plan_count=%s",
            pair_index.height,
            requested_plan_ids.height,
        )

        requested_steps = plan_steps.join(requested_plan_ids, on=plan_id_col, how="inner")
        logging.info(
            "PlanDistance: requested step rows=%s collected in %.3fs",
            requested_steps.height,
            perf_counter() - t0,
        )
        t1 = perf_counter()
        plan_features = self.build_plan_features(
            requested_steps,
            plan_id_col=plan_id_col,
            transport_zones=transport_zones,
        )
        logging.info(
            "PlanDistance: plan features built plans=%s dims=%s in %.3fs",
            plan_features.height,
            max(plan_features.width - 1, 0),
            perf_counter() - t1,
        )
        return self._compute_pair_distances_from_plan_features(
            pair_index,
            plan_features,
            plan_id_col=plan_id_col,
            dimension_weights=dimension_weights,
            total_start_time=t0,
        )

    def _compute_pair_distances_from_plan_features(
        self,
        pair_index: pl.DataFrame,
        plan_features: pl.DataFrame,
        *,
        plan_id_col: str,
        dimension_weights: list[float] | None,
        total_start_time: float,
    ) -> pl.DataFrame:
        """Join plan features onto the requested pairs and compute Euclidean distances."""

        feature_cols = [col for col in plan_features.columns if col != plan_id_col]
        effective_dimension_weights = self._get_dimension_weights(
            feature_cols,
            dimension_weights=dimension_weights,
        )
        if effective_dimension_weights is not None and len(effective_dimension_weights) != len(feature_cols):
            raise ValueError(
                "plan_embedding_dimension_weights must match the PlanDistance feature dimension. "
                f"Expected {len(feature_cols)}, got {len(dimension_weights)}."
            )
        left_features = plan_features.rename(
            {plan_id_col: "plan_id_from", **{col: f"{col}_from" for col in feature_cols}}
        )
        right_features = plan_features.rename(
            {plan_id_col: "plan_id_trans", **{col: f"{col}_trans" for col in feature_cols}}
        )

        squared_diff_exprs = []
        for idx, feature_col in enumerate(feature_cols):
            squared_diff = (pl.col(f"{feature_col}_from") - pl.col(f"{feature_col}_trans")).pow(2)
            if effective_dimension_weights is not None:
                squared_diff = squared_diff * effective_dimension_weights[idx]
            squared_diff_exprs.append(squared_diff)

        t2 = perf_counter()
        result = (
            pair_index
            .join(left_features, on="plan_id_from", how="left")
            .join(right_features, on="plan_id_trans", how="left")
            .with_columns(distance=pl.sum_horizontal(squared_diff_exprs).sqrt())
            .select(["plan_id_from", "plan_id_trans", "distance"])
        )
        stats = result.select(
            pl.col("distance").min().alias("min_distance"),
            pl.col("distance").mean().alias("mean_distance"),
            pl.col("distance").max().alias("max_distance"),
        ).row(0, named=True)
        logging.info(
            "PlanDistance: distance join+compute done in %.3fs; min=%.6f mean=%.6f max=%.6f total=%.3fs",
            perf_counter() - t2,
            float(stats["min_distance"] or 0.0),
            float(stats["mean_distance"] or 0.0),
            float(stats["max_distance"] or 0.0),
            perf_counter() - total_start_time,
        )
        return result

    def build_plan_features(
        self,
        plan_steps: pl.DataFrame,
        *,
        plan_id_col: str = "plan_id",
        transport_zones=None,
    ) -> pl.DataFrame:
        """Return one wide hourly feature row per plan."""
        t0 = perf_counter()
        has_spatial_info = transport_zones is not None and {"from", "to"}.issubset(plan_steps.columns)
        state_values = self._get_state_values(plan_steps)
        logging.info(
            "PlanDistance: build features start plans=%s step_rows=%s states=%s spatial=%s",
            plan_steps.select(plan_id_col).n_unique(),
            plan_steps.height,
            len(state_values),
            has_spatial_info,
        )

        plans = plan_steps
        if has_spatial_info:
            zone_lookup = (
                pl.DataFrame(transport_zones.get().drop("geometry", axis=1))
                .select(["transport_zone_id", "x", "y"])
                .with_columns(transport_zone_id=pl.col("transport_zone_id").cast(pl.Int32))
            )
            plans = (
                plans.join(
                    zone_lookup.rename({"transport_zone_id": "from", "x": "x_from", "y": "y_from"}),
                    on="from",
                    how="left",
                )
                .join(
                    zone_lookup.rename({"transport_zone_id": "to", "x": "x_to", "y": "y_to"}),
                    on="to",
                    how="left",
                )
            )
        logging.info("PlanDistance: zone join done in %.3fs", perf_counter() - t0)

        t1 = perf_counter()
        boundary_aggs = [
            pl.col("departure_time").min().alias("first_departure"),
            pl.col("arrival_time").max().alias("last_arrival"),
            (
                (pl.col("activity").cast(pl.String) == "home")
                & (pl.col("departure_time") <= 0.0)
                & (pl.col("arrival_time") <= 0.0)
                & (pl.col("next_departure_time") >= 24.0)
            ).any().alias("has_full_day_home_step"),
        ]
        if has_spatial_info:
            boundary_aggs.extend(
                [
                    pl.col("x_from").sort_by("seq_step_index").first().alias("home_x"),
                    pl.col("y_from").sort_by("seq_step_index").first().alias("home_y"),
                ]
            )

        boundary_segments = plans.group_by(plan_id_col).agg(boundary_aggs)

        initial_home_segments = (
            boundary_segments
            .with_columns(
                start_time=pl.lit(0.0),
                end_time=pl.col("first_departure"),
                state=pl.lit("act:home"),
            )
            .select(
                [plan_id_col, "start_time", "end_time", "state"]
                + (["home_x", "home_y"] if has_spatial_info else [])
            )
        )
        final_home_segments = (
            boundary_segments
            .with_columns(
                start_time=pl.col("last_arrival"),
                end_time=pl.when(pl.col("has_full_day_home_step")).then(pl.col("last_arrival")).otherwise(pl.lit(24.0)),
                state=pl.lit("act:home"),
            )
            .select(
                [plan_id_col, "start_time", "end_time", "state"]
                + (["home_x", "home_y"] if has_spatial_info else [])
            )
        )
        travel_segments = (
            plans.select(
                [plan_id_col, "departure_time", "arrival_time", "mode"]
                + (["x_from", "y_from", "x_to", "y_to"] if has_spatial_info else [])
            )
            .with_columns(
                start_time=pl.col("departure_time"),
                end_time=pl.col("arrival_time"),
                state=pl.concat_str([pl.lit("mode:"), pl.col("mode").cast(pl.String)]),
            )
            .select(
                [plan_id_col, "start_time", "end_time", "state"]
                + (["x_from", "y_from", "x_to", "y_to"] if has_spatial_info else [])
            )
        )
        stay_segments = (
            plans.select(
                [plan_id_col, "arrival_time", "next_departure_time", "activity"]
                + (["x_to", "y_to"] if has_spatial_info else [])
            )
            .with_columns(
                start_time=pl.col("arrival_time"),
                end_time=pl.col("next_departure_time"),
                state=pl.concat_str([pl.lit("act:"), pl.col("activity").cast(pl.String)]),
            )
            .select(
                [plan_id_col, "start_time", "end_time", "state"]
                + (["x_to", "y_to"] if has_spatial_info else [])
            )
        )

        if has_spatial_info:
            initial_home_segments = initial_home_segments.with_columns(
                x_start=pl.col("home_x"),
                y_start=pl.col("home_y"),
                x_end=pl.col("home_x"),
                y_end=pl.col("home_y"),
            ).select([plan_id_col, "start_time", "end_time", "state", "x_start", "y_start", "x_end", "y_end"])
            final_home_segments = final_home_segments.with_columns(
                x_start=pl.col("home_x"),
                y_start=pl.col("home_y"),
                x_end=pl.col("home_x"),
                y_end=pl.col("home_y"),
            ).select([plan_id_col, "start_time", "end_time", "state", "x_start", "y_start", "x_end", "y_end"])
            travel_segments = travel_segments.with_columns(
                x_start=pl.col("x_from"),
                y_start=pl.col("y_from"),
                x_end=pl.col("x_to"),
                y_end=pl.col("y_to"),
            ).select([plan_id_col, "start_time", "end_time", "state", "x_start", "y_start", "x_end", "y_end"])
            stay_segments = stay_segments.with_columns(
                x_start=pl.col("x_to"),
                y_start=pl.col("y_to"),
                x_end=pl.col("x_to"),
                y_end=pl.col("y_to"),
            ).select([plan_id_col, "start_time", "end_time", "state", "x_start", "y_start", "x_end", "y_end"])

        segments = pl.concat(
            [initial_home_segments, travel_segments, stay_segments, final_home_segments],
            how="vertical_relaxed",
        ).with_columns(
            start_time=pl.col("start_time").clip(0.0, 24.0),
            end_time=pl.col("end_time").clip(0.0, 24.0),
        ).filter(pl.col("end_time") > pl.col("start_time"))
        logging.info(
            "PlanDistance: segments built rows=%s in %.3fs",
            segments.height,
            perf_counter() - t1,
        )

        t2 = perf_counter()
        hour_bins = pl.DataFrame({"hour": list(range(24))}).with_columns(
            hour=pl.col("hour").cast(pl.Int8),
            hour_start=pl.col("hour").cast(pl.Float64),
            hour_end=(pl.col("hour") + 1).cast(pl.Float64),
        )

        segment_hours = (
            segments.join(hour_bins, how="cross")
            .with_columns(
                overlap_start=pl.max_horizontal("start_time", "hour_start"),
                overlap_end=pl.min_horizontal("end_time", "hour_end"),
            )
            .with_columns(overlap_hours=(pl.col("overlap_end") - pl.col("overlap_start")).clip(0.0))
            .filter(pl.col("overlap_hours") > 0.0)
        )
        logging.info(
            "PlanDistance: segment-hour overlaps rows=%s in %.3fs",
            segment_hours.height,
            perf_counter() - t2,
        )

        t3 = perf_counter()
        hourly_state_overlap = (
            segment_hours.group_by([plan_id_col, "state", "hour"])
            .agg(overlap_hours=pl.col("overlap_hours").sum())
            .sort([plan_id_col, "state", "hour"])
        )
        cumulative_state_hours = (
            hourly_state_overlap
            .with_columns(
                value=(pl.col("overlap_hours").cum_sum().over([plan_id_col, "state"]) / 24.0)
            )
            .with_columns(hour_str=pl.col("hour").cast(pl.String).str.zfill(2))
            .with_columns(
                feature=pl.concat_str([pl.lit("state_progress_h"), pl.col("hour_str"), pl.lit("_"), pl.col("state")]),
            )
            .select([plan_id_col, "feature", "value"])
        )
        hourly_state_progress = (
            plan_steps.select(plan_id_col).unique()
            .join(
                cumulative_state_hours.pivot(
                    on="feature",
                    index=plan_id_col,
                    values="value",
                    aggregate_function="first",
                ),
                on=plan_id_col,
                how="left",
            )
        )
        logging.info(
            "PlanDistance: state features built rows=%s cols=%s in %.3fs",
            hourly_state_progress.height,
            hourly_state_progress.width,
            perf_counter() - t3,
        )

        if has_spatial_info:
            t4 = perf_counter()
            duration_hours = (pl.col("end_time") - pl.col("start_time")).clip(1e-9)
            dx = pl.col("x_end") - pl.col("x_start")
            dy = pl.col("y_end") - pl.col("y_start")
            overlap_start_frac = ((pl.col("overlap_start") - pl.col("start_time")) / duration_hours).clip(0.0, 1.0)
            overlap_end_frac = ((pl.col("overlap_end") - pl.col("start_time")) / duration_hours).clip(0.0, 1.0)
            hourly_xy = (
                segment_hours
                .with_columns(
                    x_overlap_start=pl.col("x_start") + overlap_start_frac * dx,
                    x_overlap_end=pl.col("x_start") + overlap_end_frac * dx,
                    y_overlap_start=pl.col("y_start") + overlap_start_frac * dy,
                    y_overlap_end=pl.col("y_start") + overlap_end_frac * dy,
                )
                .with_columns(
                    x_mean=(pl.col("x_overlap_start") + pl.col("x_overlap_end")) / 2.0,
                    y_mean=(pl.col("y_overlap_start") + pl.col("y_overlap_end")) / 2.0,
                )
                .group_by([plan_id_col, "hour"])
                .agg(
                    overlap_total=pl.col("overlap_hours").sum(),
                    x_weighted=(pl.col("x_mean") * pl.col("overlap_hours")).sum(),
                    y_weighted=(pl.col("y_mean") * pl.col("overlap_hours")).sum(),
                )
                .with_columns(
                    x_hour=pl.col("x_weighted") / pl.col("overlap_total").clip(1e-9),
                    y_hour=pl.col("y_weighted") / pl.col("overlap_total").clip(1e-9),
                )
                .sort([plan_id_col, "hour"])
                .with_columns(
                    x_value=pl.col("x_hour").cum_sum().over(plan_id_col) / 24.0,
                    y_value=pl.col("y_hour").cum_sum().over(plan_id_col) / 24.0,
                    hour_str=pl.col("hour").cast(pl.String).str.zfill(2),
                )
            )
            hourly_xy_progress = pl.concat(
                [
                    hourly_xy.select(
                        [
                            plan_id_col,
                            pl.concat_str([pl.lit("x_progress_h"), pl.col("hour_str")]).alias("feature"),
                            pl.col("x_value").alias("value"),
                        ]
                    ),
                    hourly_xy.select(
                        [
                            plan_id_col,
                            pl.concat_str([pl.lit("y_progress_h"), pl.col("hour_str")]).alias("feature"),
                            pl.col("y_value").alias("value"),
                        ]
                    ),
                ],
                how="vertical_relaxed",
            ).pivot(
                on="feature",
                index=plan_id_col,
                values="value",
                aggregate_function="first",
            )
            logging.info(
                "PlanDistance: spatial features built rows=%s cols=%s in %.3fs",
                hourly_xy_progress.height,
                hourly_xy_progress.width,
                perf_counter() - t4,
            )
        else:
            hourly_xy_progress = plan_steps.select(plan_id_col).unique()

        state_feature_cols = [f"state_progress_h{hour:02d}_{state}" for state in state_values for hour in range(24)]
        xy_feature_cols = (
            [f"x_progress_h{hour:02d}" for hour in range(24)]
            + [f"y_progress_h{hour:02d}" for hour in range(24)]
            if has_spatial_info
            else []
        )
        feature_cols = state_feature_cols + xy_feature_cols
        missing_feature_cols = [
            feature
            for feature in feature_cols
            if feature not in hourly_state_progress.columns and feature not in hourly_xy_progress.columns
        ]
        if missing_feature_cols:
            hourly_state_progress = hourly_state_progress.with_columns(
                [pl.lit(0.0).alias(feature) for feature in missing_feature_cols]
            )

        result = (
            plan_steps.select(plan_id_col).unique()
            .join(hourly_state_progress, on=plan_id_col, how="left")
            .join(hourly_xy_progress, on=plan_id_col, how="left")
            .with_columns([pl.col(feature).fill_null(0.0) for feature in feature_cols])
            .select([plan_id_col] + feature_cols)
        )
        max_abs_feature = (
            result.select(
                pl.max_horizontal([pl.col(col).abs() for col in feature_cols]).max().alias("max_abs_feature")
            ).item()
            if feature_cols
            else 0.0
        )
        logging.info(
            "PlanDistance: final feature table rows=%s dims=%s max_abs=%.6f total=%.3fs",
            result.height,
            len(feature_cols),
            float(max_abs_feature or 0.0),
            perf_counter() - t0,
        )
        return result

    def _get_state_values(self, plans: pl.DataFrame) -> list[str]:
        return (
            pl.concat(
                [
                    plans.select(pl.concat_str([pl.lit("act:"), pl.col("activity").cast(pl.String)]).alias("state")),
                    plans.select(pl.concat_str([pl.lit("mode:"), pl.col("mode").cast(pl.String)]).alias("state")),
                ],
                how="vertical_relaxed",
            )
            .drop_nulls()
            .unique()
            .sort("state")["state"]
            .to_list()
        )

    def _get_dimension_weights(
        self,
        feature_cols: list[str],
        *,
        dimension_weights: list[float] | None,
    ) -> list[float] | None:
        if dimension_weights is not None:
            return dimension_weights

        spatial_weight = 1.0 / (self.spatial_scale_meters ** 2)
        return [
            spatial_weight if (feature_col.startswith("x_progress_") or feature_col.startswith("y_progress_")) else 1.0
            for feature_col in feature_cols
        ]

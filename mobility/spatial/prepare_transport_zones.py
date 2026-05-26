"""Create transport zones from building footprints with Python spatial tools."""
from __future__ import annotations

import hashlib
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from shapely.geometry import GeometryCollection
from scipy.spatial import cKDTree
from sklearn.cluster import KMeans, MiniBatchKMeans


BUILDINGS_AREA_THRESHOLD = 2e5
MIN_BUILDING_AREA = 20
MAX_BUILDING_AREA = 500e3
RNG_SEED = 0
TARGET_CRS = "EPSG:3035"
MINIBATCH_BUILDING_THRESHOLD = 20_000
DEFAULT_MAX_WORKERS = 4


def prepare_transport_zones(
    study_area_fp: str | pathlib.Path,
    osm_buildings_fp: str | pathlib.Path,
    level_of_detail: int,
    output_fp: str | pathlib.Path,
    max_workers: int | None = None,
) -> None:
    """Create transport zones and building cluster files.

    The Python backend follows the same broad method as the R backend: it
    clusters building centroids, snaps cluster centers to real buildings, builds
    Voronoi polygons, and clips them to each local admin unit.
    """
    output_fp = pathlib.Path(output_fp)
    clusters_fp, clusters_geoms_fp = _get_sidecar_paths(output_fp)

    study_area = gpd.read_file(study_area_fp, engine="pyogrio").to_crs(TARGET_CRS)
    if study_area.empty:
        raise ValueError("Cannot create transport zones from an empty study area.")

    tasks = [
        (
            lau_position,
            study_area_row.local_admin_unit_id,
            study_area_row.geometry,
            pathlib.Path(osm_buildings_fp),
            level_of_detail,
        )
        for lau_position, study_area_row in enumerate(study_area.itertuples(index=False))
    ]

    results = _run_lau_tasks(tasks, max_workers=max_workers)
    results = sorted(results, key=lambda result: result["lau_position"])

    zone_tables = [result["transport_zones"] for result in results]
    cluster_tables = [result["clusters"] for result in results]

    transport_zones = gpd.GeoDataFrame(
        pd.concat(zone_tables, ignore_index=True),
        geometry="geometry",
        crs=TARGET_CRS,
    )
    clusters = pd.concat(cluster_tables, ignore_index=True)

    transport_zones, clusters = _renumber_transport_zones(transport_zones, clusters)

    transport_zones.to_file(output_fp, driver="GPKG", index=False)
    clusters.to_parquet(clusters_fp, index=False)

    clusters_geoms = gpd.GeoDataFrame(
        clusters,
        geometry=gpd.points_from_xy(clusters["x"], clusters["y"], crs=TARGET_CRS),
        crs=TARGET_CRS,
    )
    clusters_geoms.to_file(
        clusters_geoms_fp,
        layer="cluster_buildings",
        driver="GPKG",
        index=False,
    )


def _run_lau_tasks(tasks: list[tuple], max_workers: int | None) -> list[dict]:
    if not tasks:
        return []

    max_workers = _resolve_max_workers(len(tasks), max_workers)

    results = []
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total} LAUs"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        progress_task = progress.add_task("Creating transport zones", total=len(tasks))

        if max_workers == 1:
            for task in tasks:
                results.append(_create_lau_transport_zones_worker(task))
                progress.advance(progress_task)
            return results

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_create_lau_transport_zones_worker, task) for task in tasks]
            for future in as_completed(futures):
                results.append(future.result())
                progress.advance(progress_task)
    return results


def _resolve_max_workers(task_count: int, max_workers: int | None) -> int:
    if task_count <= 0:
        return 0

    if max_workers is not None:
        return max(1, min(int(max_workers), task_count))

    return max(1, min(DEFAULT_MAX_WORKERS, task_count))


def _create_lau_transport_zones_worker(task: tuple) -> dict:
    lau_position, lau_id, lau_geom, osm_buildings_fp, level_of_detail = task

    rng = np.random.default_rng(_seed_for_lau(lau_id, lau_position))
    transport_zones, clusters = _create_lau_transport_zones(
        lau_id=lau_id,
        lau_geom=lau_geom,
        osm_buildings_fp=osm_buildings_fp,
        level_of_detail=level_of_detail,
        rng=rng,
    )
    return {
        "lau_position": lau_position,
        "transport_zones": transport_zones,
        "clusters": clusters,
    }


def _renumber_transport_zones(
    transport_zones: gpd.GeoDataFrame,
    clusters: pd.DataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    transport_zones = transport_zones.copy()
    clusters = clusters.copy()

    transport_zones["new_transport_zone_id"] = np.arange(1, len(transport_zones) + 1)
    zone_ids = transport_zones[
        ["local_admin_unit_id", "transport_zone_id", "new_transport_zone_id"]
    ]

    clusters = clusters.merge(
        zone_ids,
        on=["local_admin_unit_id", "transport_zone_id"],
        how="left",
        validate="many_to_one",
    )
    clusters = clusters.drop(columns=["transport_zone_id"]).rename(
        columns={"new_transport_zone_id": "transport_zone_id"}
    )
    transport_zones = transport_zones.drop(columns=["transport_zone_id"]).rename(
        columns={"new_transport_zone_id": "transport_zone_id"}
    )

    return transport_zones, clusters


def _create_lau_transport_zones(
    lau_id: str,
    lau_geom,
    osm_buildings_fp: pathlib.Path,
    level_of_detail: int,
    rng: np.random.Generator,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    buildings = _read_lau_buildings(osm_buildings_fp, lau_id)
    buildings = _prepare_building_centroids(buildings, lau_geom)

    if buildings.empty:
        return _create_empty_building_lau(lau_id, lau_geom)

    n_clusters = int(np.ceil(buildings["area"].sum() / BUILDINGS_AREA_THRESHOLD))

    if level_of_detail == 1 and n_clusters > 1:
        labels, medoids = _kmeans_with_nearest_building_centers(
            buildings,
            k=n_clusters,
            random_state=_rng_integer(rng),
        )
        buildings = buildings.copy()
        buildings["cluster"] = labels

        cluster_area = buildings.groupby("cluster", as_index=False)["area"].sum()
        medoids = medoids.merge(cluster_area, on="cluster", how="left")

        transport_zones = medoids.rename(
            columns={"cluster": "transport_zone_id", "X": "x", "Y": "y"}
        )
        transport_zones["weight"] = transport_zones["area"] / transport_zones["area"].sum()
        transport_zones = transport_zones.drop(columns=["area"])

        internal_distances = _compute_cluster_internal_distance(buildings, rng)
        transport_zones = transport_zones.merge(
            internal_distances,
            left_on="transport_zone_id",
            right_on="cluster",
            how="left",
        ).drop(columns=["cluster"])

        transport_zones["geometry"] = _create_voronoi_geometries(
            medoids[["cluster", "X", "Y"]],
            lau_geom,
            buildings,
        )

        k_medoids = buildings.groupby("cluster", group_keys=True).apply(
            lambda group: _compute_k_medoids(group, _rng_integer(rng)),
            include_groups=False,
        )
        k_medoids = k_medoids.reset_index(level=0).rename(
            columns={"cluster": "transport_zone_id"}
        )
    else:
        buildings = buildings.copy()
        buildings["cluster"] = 1
        internal_distances = _compute_cluster_internal_distance(buildings, rng)
        k_medoids = _compute_k_medoids(buildings, _rng_integer(rng))
        k_medoids["transport_zone_id"] = 1

        center = k_medoids.loc[k_medoids["n_clusters"] == 1].iloc[0]
        transport_zones = gpd.GeoDataFrame(
            {
                "transport_zone_id": [1],
                "weight": [1.0],
                "internal_distance": [internal_distances["internal_distance"].iloc[0]],
                "x": [center["x"]],
                "y": [center["y"]],
                "geometry": [lau_geom],
            },
            geometry="geometry",
            crs=TARGET_CRS,
        )

    transport_zones["local_admin_unit_id"] = lau_id
    k_medoids["local_admin_unit_id"] = lau_id

    transport_zones = gpd.GeoDataFrame(
        transport_zones,
        geometry="geometry",
        crs=TARGET_CRS,
    )
    k_medoids = k_medoids[
        ["n_clusters", "x", "y", "weight", "transport_zone_id", "local_admin_unit_id"]
    ]

    return transport_zones, k_medoids


def _read_lau_buildings(osm_buildings_fp: pathlib.Path, lau_id: str) -> gpd.GeoDataFrame:
    building_fp = osm_buildings_fp / lau_id / "building.pbf"
    return gpd.read_file(
        building_fp,
        layer="multipolygons",
        columns=["osm_id"],
        engine="pyogrio",
    )


def _prepare_building_centroids(buildings: gpd.GeoDataFrame, lau_geom) -> pd.DataFrame:
    if buildings.empty:
        return pd.DataFrame(columns=["area", "X", "Y", "building_id"])

    buildings = buildings.to_crs(TARGET_CRS)
    geometry = buildings.geometry.array
    area = shapely.area(geometry)
    keep = (area > MIN_BUILDING_AREA) & (area < MAX_BUILDING_AREA)

    if not np.any(keep):
        return pd.DataFrame(columns=["area", "X", "Y", "building_id"])

    geometry = geometry[keep]
    centroids = shapely.centroid(geometry)
    x = shapely.get_x(centroids)
    y = shapely.get_y(centroids)

    inside = shapely.intersects(centroids, lau_geom)
    return pd.DataFrame(
        {
            "area": area[keep][inside].astype(float),
            "X": x[inside].astype(float),
            "Y": y[inside].astype(float),
        }
    ).assign(building_id=lambda df: np.arange(1, len(df) + 1))


def _create_empty_building_lau(
    lau_id: str,
    lau_geom,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    point = shapely.point_on_surface(lau_geom)
    x = float(shapely.get_x(point))
    y = float(shapely.get_y(point))

    transport_zones = gpd.GeoDataFrame(
        {
            "transport_zone_id": [1],
            "weight": [1.0],
            "internal_distance": [0.0],
            "x": [x],
            "y": [y],
            "local_admin_unit_id": [lau_id],
            "geometry": [lau_geom],
        },
        geometry="geometry",
        crs=TARGET_CRS,
    )
    clusters = pd.DataFrame(
        {
            "n_clusters": [1],
            "x": [x],
            "y": [y],
            "weight": [1.0],
            "transport_zone_id": [1],
            "local_admin_unit_id": [lau_id],
        }
    )
    return transport_zones, clusters


def _kmeans_with_nearest_building_centers(
    buildings: pd.DataFrame,
    k: int,
    random_state: int,
) -> tuple[np.ndarray, pd.DataFrame]:
    coords = buildings[["X", "Y"]].to_numpy()
    n_unique = len(np.unique(coords, axis=0))
    k = max(1, min(int(k), len(coords), n_unique))

    if k == 1:
        return _one_cluster_labels_and_medoids(buildings)

    if len(coords) >= MINIBATCH_BUILDING_THRESHOLD:
        model = MiniBatchKMeans(
            n_clusters=k,
            random_state=random_state,
            n_init=1,
            batch_size=4096,
        )
    else:
        model = KMeans(
            n_clusters=k,
            random_state=random_state,
            n_init=1,
            max_iter=10,
        )

    labels = model.fit_predict(coords)
    return _snap_centers_to_buildings(coords, labels, model.cluster_centers_)


def _one_cluster_labels_and_medoids(buildings: pd.DataFrame) -> tuple[np.ndarray, pd.DataFrame]:
    coords = buildings[["X", "Y"]].to_numpy()
    medoid_idx = _nearest_to_weighted_center(buildings)
    medoids = pd.DataFrame(
        {
            "cluster": [1],
            "X": [coords[medoid_idx, 0]],
            "Y": [coords[medoid_idx, 1]],
        }
    )
    return np.ones(len(coords), dtype=int), medoids


def _snap_centers_to_buildings(
    coords: np.ndarray,
    labels: np.ndarray,
    centers: np.ndarray,
) -> tuple[np.ndarray, pd.DataFrame]:
    """Snap centers to real buildings and return stable one-based labels."""
    tree = cKDTree(coords)
    _, medoid_idx = tree.query(centers, k=1)

    order = np.argsort(medoid_idx)
    old_to_new = {
        old_label: new_label
        for new_label, old_label in enumerate(order, start=1)
    }
    labels = np.array([old_to_new[label] for label in labels], dtype=int)
    medoid_idx = medoid_idx[order]

    medoids = pd.DataFrame(
        {
            "cluster": np.arange(1, len(medoid_idx) + 1),
            "X": coords[medoid_idx, 0],
            "Y": coords[medoid_idx, 1],
        }
    )
    return labels, medoids


def _nearest_to_weighted_center(buildings: pd.DataFrame) -> int:
    coords = buildings[["X", "Y"]].to_numpy()
    weights = buildings["area"].to_numpy()
    center = np.average(coords, axis=0, weights=weights)
    tree = cKDTree(coords)
    _, idx = tree.query(center, k=1)
    return int(idx)


def _compute_cluster_internal_distance(
    buildings: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    rows = []
    for cluster, group in buildings.groupby("cluster", sort=True):
        coords = group[["X", "Y"]].to_numpy()
        probabilities = group["area"].to_numpy(dtype=float)
        probabilities = probabilities / probabilities.sum()

        from_idx = rng.choice(len(group), size=1000, replace=True, p=probabilities)
        to_idx = rng.choice(len(group), size=1000, replace=True, p=probabilities)
        distances = np.linalg.norm(coords[from_idx] - coords[to_idx], axis=1)
        distances = distances * (1.1 + 0.3 * np.exp(-distances / 20))
        rows.append(
            {
                "cluster": cluster,
                "internal_distance": float(np.median(distances)),
            }
        )
    return pd.DataFrame(rows)


def _compute_k_medoids(buildings: pd.DataFrame, random_state: int) -> pd.DataFrame:
    rows = []
    n_buildings = len(buildings)

    for requested_k in range(1, 6):
        k = max(1, min(requested_k, n_buildings // 10))
        labels, medoids = _small_kmeans_with_nearest_building_centers(
            buildings,
            k=k,
            random_state=random_state + requested_k,
        )

        area_by_subcluster = np.bincount(
            labels,
            weights=buildings["area"].to_numpy(dtype=float),
            minlength=len(medoids) + 1,
        )[1:]
        current = medoids.rename(columns={"cluster": "subcluster", "X": "x", "Y": "y"})
        current["weight"] = area_by_subcluster / area_by_subcluster.sum()
        current["n_clusters"] = requested_k
        rows.append(current[["n_clusters", "x", "y", "weight"]])

    return pd.concat(rows, ignore_index=True)


def _small_kmeans_with_nearest_building_centers(
    buildings: pd.DataFrame,
    k: int,
    random_state: int,
    iter_max: int = 10,
) -> tuple[np.ndarray, pd.DataFrame]:
    """Run a small NumPy k-means for routing medoids.

    Routing medoids ask for at most five centers. Using scikit-learn for these
    small repeated fits costs more in setup than in math, so this helper keeps
    the same simple Lloyd method in NumPy.
    """
    coords = buildings[["X", "Y"]].to_numpy(dtype=float)
    n_unique = len(np.unique(coords, axis=0))
    k = max(1, min(int(k), len(coords), n_unique))

    if k == 1:
        return _one_cluster_labels_and_medoids(buildings)

    rng = np.random.default_rng(random_state)
    _, unique_indices = np.unique(coords, axis=0, return_index=True)
    center_indices = rng.choice(unique_indices, size=k, replace=False)
    centers = coords[center_indices].copy()

    raw_labels = None
    for _ in range(iter_max):
        distances = np.sum((coords[:, None, :] - centers[None, :, :]) ** 2, axis=2)
        new_labels = np.argmin(distances, axis=1)

        if raw_labels is not None and np.array_equal(raw_labels, new_labels):
            break

        raw_labels = new_labels
        for cluster_index in range(k):
            in_cluster = raw_labels == cluster_index
            if np.any(in_cluster):
                centers[cluster_index] = coords[in_cluster].mean(axis=0)

    return _snap_centers_to_buildings(coords, raw_labels, centers)


def _create_voronoi_geometries(
    medoids: pd.DataFrame,
    lau_geom,
    buildings: pd.DataFrame,
) -> list:
    points = shapely.points(medoids["X"].to_numpy(), medoids["Y"].to_numpy())
    points_collection = GeometryCollection(points.tolist())
    envelope = shapely.box(
        buildings["X"].min(),
        buildings["Y"].min(),
        buildings["X"].max(),
        buildings["Y"].max(),
    )
    envelope = shapely.buffer(envelope, 100_000)

    voronoi = shapely.voronoi_polygons(points_collection, extend_to=envelope)
    polygons = list(shapely.get_parts(voronoi))
    clipped = shapely.intersection(np.array(polygons, dtype=object), lau_geom)

    voronoi_gdf = gpd.GeoDataFrame(
        {"geometry": clipped},
        geometry="geometry",
        crs=TARGET_CRS,
    )
    points_gdf = gpd.GeoDataFrame(
        medoids[["cluster"]].copy(),
        geometry=gpd.points_from_xy(medoids["X"], medoids["Y"], crs=TARGET_CRS),
        crs=TARGET_CRS,
    )
    joined = gpd.sjoin(points_gdf, voronoi_gdf.reset_index(), predicate="intersects")
    joined = joined.sort_values(["cluster", "index"]).drop_duplicates("cluster")
    geometry_by_cluster = joined.set_index("cluster")["index"].to_dict()

    return [
        voronoi_gdf.geometry.iloc[geometry_by_cluster[cluster]]
        for cluster in medoids["cluster"]
    ]


def _get_sidecar_paths(output_fp: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    stem = output_fp.stem
    return (
        output_fp.parent / f"{stem}_buildings.parquet",
        output_fp.parent / f"{stem}_buildings_geoms.gpkg",
    )


def _seed_for_lau(lau_id: str, lau_position: int) -> int:
    stable_hash = hashlib.blake2b(lau_id.encode("utf-8"), digest_size=4).digest()
    lau_seed = int.from_bytes(stable_hash, "little")
    return int((RNG_SEED + lau_seed + lau_position) % np.iinfo(np.int32).max)


def _rng_integer(rng: np.random.Generator) -> int:
    return int(rng.integers(0, np.iinfo(np.int32).max))

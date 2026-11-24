import logging
import os
import pathlib

import geopandas as gpd
import pandas as pd
import shapely
from shapely.ops import unary_union

from mobility.runtime.assets.file_asset import FileAsset
from mobility.spatial.admin_datasets import AdminExpressDataset, BKGBoundariesDataset, SwissTopoBoundariesDataset
from mobility.spatial.selected_rows import read_selected_rows


FRENCH_ADMIN_LEVELS = {"country", "region", "departement", "epci", "commune"}
SWISS_ADMIN_LEVELS = {"country", "municipality"}
GERMAN_ADMIN_LEVELS = {"country", "gemeinde"}


def add_bbox_columns(admin_units: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add longitude and latitude bbox columns while keeping the input geometry CRS."""
    bounds = admin_units.to_crs(4326).bounds
    admin_units = admin_units.copy()
    admin_units["minx"] = bounds["minx"]
    admin_units["miny"] = bounds["miny"]
    admin_units["maxx"] = bounds["maxx"]
    admin_units["maxy"] = bounds["maxy"]
    return admin_units


def require_columns(dataframe: pd.DataFrame, columns: list[str], layer_name: str) -> None:
    """Fail clearly when a source layer does not have the expected columns."""
    missing_columns = [column for column in columns if column not in dataframe.columns]
    if len(missing_columns) > 0:
        raise ValueError(f"{layer_name} is missing columns: {missing_columns}.")


def read_selected_admin_units(admin_units_asset: FileAsset, admin_ids: list[str]) -> gpd.GeoDataFrame:
    """Read only the requested admin units when the prepared file can do it."""
    columns = ["admin_level", "admin_id", "admin_name", "country", "geometry"]
    selected_ids = sorted(set(str(admin_id) for admin_id in admin_ids if admin_id is not None))
    if len(selected_ids) == 0:
        return gpd.GeoDataFrame(columns=columns, geometry="geometry", crs=3035)

    if admin_units_asset.is_update_needed():
        admin_units = admin_units_asset.get()
    else:
        try:
            admin_units = gpd.read_parquet(
                admin_units_asset.cache_path,
                filters=[("admin_id", "in", selected_ids)],
            )
        except (TypeError, ValueError):
            admin_units = gpd.read_parquet(admin_units_asset.cache_path)

    return admin_units[admin_units["admin_id"].isin(selected_ids)].copy()


def read_admin_units_within_bounds(admin_units_asset: FileAsset, bounds: tuple[float, float, float, float]) -> gpd.GeoDataFrame:
    """Read admin units whose bounding boxes touch the given lon-lat bounds."""
    minx, miny, maxx, maxy = bounds

    # If the prepared file does not exist yet, build it once then filter it.
    if admin_units_asset.is_update_needed():
        admin_units = admin_units_asset.get()
    else:
        try:
            admin_units = gpd.read_parquet(
                admin_units_asset.cache_path,
                filters=[
                    ("minx", "<=", maxx),
                    ("maxx", ">=", minx),
                    ("miny", "<=", maxy),
                    ("maxy", ">=", miny),
                ],
            )
        except (TypeError, ValueError):
            admin_units = gpd.read_parquet(admin_units_asset.cache_path)

    return keep_admin_units_within_bounds(admin_units, bounds)


def keep_admin_units_within_bounds(
    admin_units: gpd.GeoDataFrame,
    bounds: tuple[float, float, float, float],
) -> gpd.GeoDataFrame:
    """Keep admin units whose stored lon-lat bounding boxes touch the bounds."""
    minx, miny, maxx, maxy = bounds
    bbox_columns = {"minx", "miny", "maxx", "maxy"}
    if not bbox_columns.issubset(admin_units.columns):
        admin_units = add_bbox_columns(admin_units)

    selected = (
        (admin_units["minx"] <= maxx)
        & (admin_units["maxx"] >= minx)
        & (admin_units["miny"] <= maxy)
        & (admin_units["maxy"] >= miny)
    )
    return admin_units[selected].copy()


class FrenchAdminUnits(FileAsset):
    """French administrative units prepared from ADMIN EXPRESS."""

    level_to_layer = {
        "region": "REGION.shp",
        "departement": "DEPARTEMENT.shp",
        "epci": "EPCI.shp",
        "commune": "COMMUNE.shp",
    }
    level_columns = {
        "region": ("INSEE_REG", "NOM"),
        "departement": ("INSEE_DEP", "NOM"),
        "epci": ("CODE_SIREN", "NOM"),
    }

    def __init__(self, level: str):
        level = str(level).lower()
        if level not in FRENCH_ADMIN_LEVELS:
            raise ValueError(f"Unsupported French admin level: {level}.")

        inputs = {
            "admin_express": AdminExpressDataset(),
            "level": level,
        }
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "ign"
            / "admin-express"
            / f"french_admin_units_{level}.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        logging.info("French %s admin units already prepared. Reusing %s.", self.level, self.cache_path)
        return gpd.read_parquet(self.cache_path)

    def get_by_ids(self, admin_ids: list[str]) -> gpd.GeoDataFrame:
        """Return only the requested French admin units."""
        return read_selected_admin_units(self, admin_ids)

    def get_within_bounds(self, bounds: tuple[float, float, float, float]) -> gpd.GeoDataFrame:
        """Return French admin units whose bounding boxes touch the bounds."""
        return read_admin_units_within_bounds(self, bounds)

    def create_and_get_asset(self) -> gpd.GeoDataFrame:
        logging.info("Preparing French %s admin units.", self.level)

        if self.level == "country":
            admin_units = self.prepare_country()
        elif self.level == "commune":
            admin_units = self.prepare_communes()
        else:
            admin_units = self.prepare_layer(self.level)

        admin_units = add_bbox_columns(admin_units)
        admin_units.to_parquet(self.cache_path)
        return admin_units

    def prepare_layer(self, level: str) -> gpd.GeoDataFrame:
        source_path = self.inputs["admin_express"].get()
        layer_name = self.level_to_layer[level]
        layer = gpd.read_file(source_path / layer_name)

        id_column, name_column = self.level_columns[level]
        require_columns(layer, [id_column, name_column, "geometry"], layer_name)

        admin_units = layer[[id_column, name_column, "geometry"]].copy()
        admin_units.columns = ["admin_id", "admin_name", "geometry"]
        admin_units["admin_level"] = level
        admin_units["country"] = "fr"
        admin_units["admin_id"] = "fr-" + admin_units["admin_id"].astype(str)
        admin_units = admin_units.to_crs(3035)
        return admin_units[
            ["admin_level", "admin_id", "admin_name", "country", "geometry"]
        ]

    def prepare_country(self) -> gpd.GeoDataFrame:
        regions = self.prepare_layer("region")
        return gpd.GeoDataFrame(
            {
                "admin_level": ["country"],
                "admin_id": ["fr-FR"],
                "admin_name": ["France"],
                "country": ["fr"],
            },
            geometry=[unary_union(regions.geometry)],
            crs=regions.crs,
        )

    def prepare_communes(self) -> gpd.GeoDataFrame:
        source_path = self.inputs["admin_express"].get()
        arrond = gpd.read_file(source_path / "ARRONDISSEMENT_MUNICIPAL.shp")
        communes = gpd.read_file(source_path / "COMMUNE.shp")

        commune_columns = [
            "INSEE_COM",
            "INSEE_CAN",
            "NOM",
            "SIREN_EPCI",
            "INSEE_DEP",
            "INSEE_REG",
            "geometry",
        ]
        require_columns(communes, commune_columns, "COMMUNE.shp")
        require_columns(
            arrond,
            ["INSEE_COM", "INSEE_ARM", "NOM", "geometry"],
            "ARRONDISSEMENT_MUNICIPAL.shp",
        )
        communes = communes[commune_columns].copy()
        communes.loc[communes["INSEE_CAN"] == "NR", "INSEE_CAN"] = "ZZ"
        communes["INSEE_CAN"] = communes["INSEE_DEP"] + communes["INSEE_CAN"]

        arrond = arrond[["INSEE_COM", "INSEE_ARM", "NOM", "geometry"]].copy()
        arrond = arrond.merge(
            pd.DataFrame(communes.drop(columns="geometry")),
            on="INSEE_COM",
            how="left",
            suffixes=("", "_parent"),
        )

        parent_commune_ids = set(arrond["INSEE_COM"].dropna().astype(str))
        communes = communes[~communes["INSEE_COM"].astype(str).isin(parent_commune_ids)].copy()
        communes["parent_commune_id"] = communes["INSEE_COM"].astype(str)
        communes["raw_commune_id"] = communes["INSEE_COM"].astype(str)

        arrond["parent_commune_id"] = arrond["INSEE_COM"].astype(str)
        arrond["raw_commune_id"] = arrond["INSEE_ARM"].astype(str)
        arrond["INSEE_CAN"] = arrond["INSEE_ARM"].str[0:2] + "ZZ"
        arrond.loc[arrond["INSEE_ARM"].str[0:2] == "13", "INSEE_CAN"] = "1398"
        arrond["INSEE_COM"] = arrond["INSEE_ARM"]

        communes = pd.concat(
            [
                communes[
                    [
                        "INSEE_COM",
                        "INSEE_CAN",
                        "NOM",
                        "SIREN_EPCI",
                        "INSEE_DEP",
                        "INSEE_REG",
                        "parent_commune_id",
                        "raw_commune_id",
                        "geometry",
                    ]
                ],
                arrond[
                    [
                        "INSEE_COM",
                        "INSEE_CAN",
                        "NOM",
                        "SIREN_EPCI",
                        "INSEE_DEP",
                        "INSEE_REG",
                        "parent_commune_id",
                        "raw_commune_id",
                        "geometry",
                    ]
                ],
            ]
        )

        communes["admin_level"] = "commune"
        communes["admin_id"] = "fr-" + communes["INSEE_COM"].astype(str)
        communes["admin_name"] = communes["NOM"]
        communes["country"] = "fr"
        communes["commune_id"] = "fr-" + communes["raw_commune_id"].astype(str)
        communes["parent_commune_id"] = "fr-" + communes["parent_commune_id"].astype(str)
        communes["canton_id"] = communes["INSEE_CAN"]
        grand_paris = communes["SIREN_EPCI"].astype(str).str[0:9] == "200054781"
        communes.loc[grand_paris, "SIREN_EPCI"] = "200054781"
        communes["epci_id"] = "fr-" + communes["SIREN_EPCI"].astype(str)
        communes["departement_id"] = "fr-" + communes["INSEE_DEP"].astype(str)
        communes["region_id"] = "fr-" + communes["INSEE_REG"].astype(str)
        communes = communes.to_crs(3035)

        return communes[
            [
                "admin_level",
                "admin_id",
                "admin_name",
                "country",
                "commune_id",
                "parent_commune_id",
                "canton_id",
                "epci_id",
                "departement_id",
                "region_id",
                "geometry",
            ]
        ]

    @classmethod
    def get_population_commune_boundaries(cls):
        """Return commune boundaries with columns used by French population data."""
        cities = cls(level="commune").get()
        cities = cities[
            ["admin_id", "canton_id", "epci_id", "admin_name", "geometry"]
        ].copy()
        cities.columns = ["INSEE_COM", "INSEE_CAN", "SIREN_EPCI", "NOM", "geometry"]
        cities["SIREN_EPCI"] = cities["SIREN_EPCI"].str.removeprefix("fr-")
        return cities

    @classmethod
    def get_population_region_boundaries(cls):
        """Return region boundaries with columns used by French population data."""
        regions = cls(level="region").get()
        regions = regions[["admin_id", "admin_name", "geometry"]].copy()
        regions.columns = ["INSEE_REG", "NOM", "geometry"]
        regions["INSEE_REG"] = regions["INSEE_REG"].str.removeprefix("fr-")
        return regions


class SwissAdminUnits(FileAsset):
    """Swiss administrative units prepared from swisstopo boundaries."""

    def __init__(self, level: str):
        level = str(level).lower()
        if level not in SWISS_ADMIN_LEVELS:
            raise ValueError(f"Unsupported Swiss admin level: {level}.")

        inputs = {
            "swiss_topo_boundaries": SwissTopoBoundariesDataset(),
            "level": level,
        }
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "swisstopo"
            / f"swiss_admin_units_{level}.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        logging.info("Swiss %s admin units already prepared. Reusing %s.", self.level, self.cache_path)
        return gpd.read_parquet(self.cache_path)

    def get_by_ids(self, admin_ids: list[str]) -> gpd.GeoDataFrame:
        """Return only the requested Swiss admin units."""
        return read_selected_admin_units(self, admin_ids)

    def get_within_bounds(self, bounds: tuple[float, float, float, float]) -> gpd.GeoDataFrame:
        """Return Swiss admin units whose bounding boxes touch the bounds."""
        return read_admin_units_within_bounds(self, bounds)

    def create_and_get_asset(self) -> gpd.GeoDataFrame:
        logging.info("Preparing Swiss %s admin units.", self.level)

        municipalities = self.prepare_municipalities()
        if self.level == "municipality":
            admin_units = municipalities
        else:
            admin_units = self.prepare_country(municipalities)

        admin_units = add_bbox_columns(admin_units)
        admin_units.to_parquet(self.cache_path)
        return admin_units

    def prepare_municipalities(self) -> gpd.GeoDataFrame:
        gpkg_path = self.inputs["swiss_topo_boundaries"].get()
        municipalities = gpd.read_file(gpkg_path, layer="tlm_hoheitsgebiet")
        municipalities = municipalities[["bfs_nummer", "name", "geometry"]].copy()
        municipalities.columns = ["admin_id", "admin_name", "geometry"]
        municipalities["admin_level"] = "municipality"
        municipalities["country"] = "ch"
        municipalities["admin_id"] = "ch-" + municipalities["admin_id"].astype(str)
        municipalities["geometry"] = shapely.wkb.loads(
            shapely.wkb.dumps(municipalities["geometry"], output_dimension=2)
        )
        municipalities = municipalities.to_crs(3035)
        return municipalities[
            ["admin_level", "admin_id", "admin_name", "country", "geometry"]
        ]

    def prepare_country(self, municipalities: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(
            {
                "admin_level": ["country"],
                "admin_id": ["ch-CH"],
                "admin_name": ["Switzerland"],
                "country": ["ch"],
            },
            geometry=[unary_union(municipalities.geometry)],
            crs=municipalities.crs,
        )


class GermanAdminUnits(FileAsset):
    """German administrative units prepared from bkg boundaries."""

    def __init__(self, level: str):
        level = str(level).lower()
        if level not in GERMAN_ADMIN_LEVELS:
            raise ValueError(f"Unsupported German admin level: {level}.")

        inputs = {
            "bkg_boundaries": BKGBoundariesDataset(),
            "level": level,
        }
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bkg"
            / f"german_admin_units_{level}.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        logging.info("German %s admin units already prepared. Reusing %s.", self.level, self.cache_path)
        return gpd.read_parquet(self.cache_path)

    def get_by_ids(self, admin_ids: list[str]) -> gpd.GeoDataFrame:
        """Return only the requested German admin units."""
        return read_selected_admin_units(self, admin_ids)

    def get_within_bounds(self, bounds: tuple[float, float, float, float]) -> gpd.GeoDataFrame:
        """Return German admin units whose bounding boxes touch the bounds."""
        return read_admin_units_within_bounds(self, bounds)

    def create_and_get_asset(self) -> gpd.GeoDataFrame:
        logging.info("Preparing German %s admin units.", self.level)

        gemeinden = self.prepare_gemeinden()
        if self.level == "gemeinde":
            admin_units = gemeinden
        else:
            admin_units = self.prepare_country(gemeinden)

        admin_units = add_bbox_columns(admin_units)
        admin_units.to_parquet(self.cache_path)
        return admin_units

    def prepare_gemeinden(self) -> gpd.GeoDataFrame:
        gpkg_path = self.inputs["bkg_boundaries"].get()
        gemeinden = gpd.read_file(gpkg_path, layer="vg250_gem")
        gemeinden = gemeinden[["ARS", "GEN", "geometry"]].copy()
        gemeinden.columns = ["admin_id", "admin_name", "geometry"]
        gemeinden["admin_level"] = "gemeinde"
        gemeinden["country"] = "de"
        gemeinden["admin_id"] = "de-" + gemeinden["admin_id"].astype(str)
        gemeinden["geometry"] = shapely.wkb.loads(
            shapely.wkb.dumps(gemeinden["geometry"], output_dimension=2)
        )
        gemeinden = gemeinden.to_crs(3035)
        return gemeinden[
            ["admin_level", "admin_id", "admin_name", "country", "geometry"]
        ]

    def prepare_country(self, gemeinden: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(
            {
                "admin_level": ["country"],
                "admin_id": ["de-DE"],
                "admin_name": ["Deutschland"],
                "country": ["ch"],
            },
            geometry=[unary_union(gemeinden.geometry)],
            crs=gemeinden.crs,
        )

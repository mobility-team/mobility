import logging
import os
import pathlib
import zipfile

import py7zr

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file


class AdminExpressDataset(FileAsset):
    """Downloaded and extracted French ADMIN EXPRESS dataset."""

    archive_name = "ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2024-02-22.7z"
    extracted_name = "ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2024-02-22"
    delivery_name = "1_DONNEES_LIVRAISON_2024-03-00169"
    shp_name = "ADECOGC_3-2_SHP_LAMB93_FXX-ED2024-02-22"
    url = (
        "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS-COG-CARTO/"
        f"{extracted_name}/{archive_name}"
    )

    def __init__(self):
        inputs = {
            "url": self.url,
            "archive_name": self.archive_name,
            "extracted_name": self.extracted_name,
        }
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "ign"
            / "admin-express"
            / "admin_express_dataset.ready"
        )
        super().__init__(inputs, cache_path)

    @property
    def archive_path(self) -> pathlib.Path:
        return pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign" / "admin-express" / self.archive_name

    @property
    def extracted_path(self) -> pathlib.Path:
        return (
            self.archive_path.parent
            / self.extracted_name
            / "ADMIN-EXPRESS-COG-CARTO"
            / self.delivery_name
            / self.shp_name
        )

    @property
    def expected_files(self) -> list[pathlib.Path]:
        return [
            self.extracted_path / "COMMUNE.shp",
            self.extracted_path / "ARRONDISSEMENT_MUNICIPAL.shp",
            self.extracted_path / "EPCI.shp",
            self.extracted_path / "DEPARTEMENT.shp",
            self.extracted_path / "REGION.shp",
        ]

    def get_cached_asset(self) -> pathlib.Path:
        logging.info("ADMIN EXPRESS already downloaded. Reusing %s.", self.extracted_path)
        return self.extracted_path

    def assets_missing(self) -> bool:
        """Return True when the marker or expected ADMIN EXPRESS files are missing."""
        return super().assets_missing() or any(
            not expected_file.exists() for expected_file in self.expected_files
        )

    def create_and_get_asset(self) -> pathlib.Path:
        logging.info("Downloading and extracting ADMIN EXPRESS.")
        download_file(self.url, self.archive_path)

        with py7zr.SevenZipFile(self.archive_path, "r") as archive:
            archive.extractall(self.archive_path.parent)

        self.cache_path.write_text(str(self.extracted_path), encoding="utf-8")
        return self.extracted_path


class SwissTopoBoundariesDataset(FileAsset):
    """Downloaded and extracted swisstopo boundaries dataset."""

    archive_name = "swissboundaries3d_2024-01_2056_5728.gpkg.zip"
    gpkg_name = "swissBOUNDARIES3D_1_5_LV95_LN02.gpkg"
    url = (
        "https://data.geo.admin.ch/ch.swisstopo.swissboundaries3d/"
        f"swissboundaries3d_2024-01/{archive_name}"
    )

    def __init__(self):
        inputs = {
            "url": self.url,
            "archive_name": self.archive_name,
            "gpkg_name": self.gpkg_name,
        }
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "swisstopo"
            / "swiss_topo_boundaries_dataset.ready"
        )
        super().__init__(inputs, cache_path)

    @property
    def archive_path(self) -> pathlib.Path:
        return pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "swisstopo" / self.archive_name

    @property
    def gpkg_path(self) -> pathlib.Path:
        return self.archive_path.parent / self.gpkg_name

    def get_cached_asset(self) -> pathlib.Path:
        logging.info("swisstopo boundaries already downloaded. Reusing %s.", self.gpkg_path)
        return self.gpkg_path

    def assets_missing(self) -> bool:
        """Return True when the marker or extracted swisstopo file is missing."""
        return super().assets_missing() or not self.gpkg_path.exists()

    def create_and_get_asset(self) -> pathlib.Path:
        logging.info("Downloading and extracting swisstopo boundaries.")
        download_file(self.url, self.archive_path)

        with zipfile.ZipFile(self.archive_path, "r") as archive:
            archive.extractall(self.archive_path.parent)

        self.cache_path.write_text(str(self.gpkg_path), encoding="utf-8")
        return self.gpkg_path


class BKGBoundariesDataset(FileAsset):
    """Downloaded and extracted bkg boundaries dataset."""

    archive_name = "swissboundaries3d_2024-01_2056_5728.gpkg.zip" # TODO
    gpkg_name = "DE_VG250.gpkg"
    url = (
        "https://data.geo.admin.ch/ch.swisstopo.swissboundaries3d/" # TODO
        f"swissboundaries3d_2024-01/{archive_name}"
    )

    def __init__(self):
        inputs = {
            "url": self.url,
            "archive_name": self.archive_name,
            "gpkg_name": self.gpkg_name,
        }
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bkg"
            / "bkg_boundaries_dataset.ready"
        )
        super().__init__(inputs, cache_path)

    @property
    def archive_path(self) -> pathlib.Path:
        return pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "germany" / self.archive_name

    @property
    def gpkg_path(self) -> pathlib.Path:
        return self.archive_path.parent / self.gpkg_name

    def get_cached_asset(self) -> pathlib.Path:
        logging.info("bkg boundaries already downloaded. Reusing %s.", self.gpkg_path)
        return self.gpkg_path

    def assets_missing(self) -> bool:
        """Return True when the marker or extracted bkg file is missing."""
        return super().assets_missing() or not self.gpkg_path.exists()

    def create_and_get_asset(self) -> pathlib.Path:
        logging.info("Downloading and extracting bkg boundaries.")
        # download_file(self.url, self.archive_path)

        # with zipfile.ZipFile(self.archive_path, "r") as archive:
        #     archive.extractall(self.archive_path.parent)

        self.cache_path.write_text(str(self.gpkg_path), encoding="utf-8")
        return self.gpkg_path

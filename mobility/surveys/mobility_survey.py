from __future__ import annotations
import os
import pathlib
import pandas as pd
from typing import Annotated, Any
from pydantic import BaseModel, ConfigDict, Field
from mobility.runtime.assets.file_asset import FileAsset


class MobilitySurvey(FileAsset):
    """
    A class for managing and processing mobility survey data for the EMP-2019 and ENTD-2008 surveys.
    
    Attributes:
        source (str): The source of the mobility survey data (e.g., "fr-EMP-2019", "fr-ENTD-2008", "ch-MRMT-2021").
        cache_path (dict): A dictionary mapping data identifiers to their file paths in the cache.
    
    Methods:
        get_cached_asset: Returns the cached asset data as a dictionary of pandas DataFrames.
    """
    
    def __init__(
        self,
        survey_name: str | None = None,
        country: str | None = None,
        parameters: "MobilitySurveyParameters" | None = None,
    ):
        """Initialize mobility survey asset inputs and cache paths.

        Args:
            survey_name: Survey dataset identifier.
            country: Country code associated with the survey.
            parameters: Optional pre-built pydantic parameters model.
        """
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=MobilitySurveyParameters,
            explicit_args={
                "survey_name": survey_name,
                "country": country,
            },
            required_fields=["survey_name", "country"],
            owner_name="MobilitySurvey",
        )

        folder_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "mobility_surveys"
            / parameters.survey_name
        )
        
        files = {
            "short_trips": "short_dist_trips.parquet",
            "days_trip": "days_trip.parquet",
            "long_trips": "long_dist_trips.parquet",
            "travels": "travels.parquet",
            "n_travels": "long_dist_travel_number.parquet",
            "p_immobility": "immobility_probability.parquet",
            "p_car": "car_ownership_probability.parquet",
            "p_det_mode": "insee_modes_to_entd_modes.parquet"
        }
        
        cache_path = {k: folder_path / file for k, file in files.items()}
        inputs = {
            "version": "1",
            "parameters": parameters,
        }
        inputs.update(self.get_additional_inputs())
        super().__init__(inputs, cache_path)

    def get_additional_inputs(self) -> dict[str, Any]:
        """Hook for specialized parsers to add extra hashed inputs before cache initialization."""
        return {}
        
    def get_cached_asset(self) -> dict[str, pd.DataFrame]:
        """
        Fetches the cached survey data.
        
        Returns:
            dict: A dictionary where keys are data identifiers and values are pandas DataFrames of the cached data.
        """
        return {k: pd.read_parquet(path) for k, path in self.cache_path.items()}
    
class MobilitySurveyParameters(BaseModel):
    """Parameters used to configure a mobility survey asset."""

    model_config = ConfigDict(extra="forbid")

    survey_name: Annotated[
        str,
        Field(
            title="Survey name",
            description="Identifier of the survey dataset folder to load.",
        ),
    ]

    country: Annotated[
        str,
        Field(
            title="Country code",
            description="ISO-like country code used to map surveys to population inputs.",
        ),
    ]

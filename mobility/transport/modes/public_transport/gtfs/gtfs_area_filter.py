from dataclasses import dataclass

import geopandas as gpd

from mobility.spatial.admin_units import FrenchAdminUnits


@dataclass
class FrenchGTFSAreaFilter:
    """Code sets used to prefilter French GTFS datasets by covered_area."""

    countries: set[str]
    regions: set[str]
    departements: set[str]
    epcis: set[str]
    communes: set[str]
    disabled: bool = False
    known_area_types = {"pays", "region", "departement", "epci", "commune"}

    @classmethod
    def from_transport_zones(cls, transport_zones: gpd.GeoDataFrame):
        """Build a French area filter from transport-zone local admin unit ids."""
        local_admin_unit_ids = transport_zones["local_admin_unit_id"].astype(str)
        french_zone_ids = set(local_admin_unit_ids[local_admin_unit_ids.str.startswith("fr-")])
        if len(french_zone_ids) == 0:
            return cls(
                countries=set(),
                regions=set(),
                departements=set(),
                epcis=set(),
                communes=set(),
                disabled=True,
            )

        communes = FrenchAdminUnits(level="commune").get()
        selected_communes = communes.loc[communes["admin_id"].isin(french_zone_ids)]
        if selected_communes.empty:
            return cls(
                countries=set(),
                regions=set(),
                departements=set(),
                epcis=set(),
                communes=set(),
                disabled=True,
            )

        commune_ids = selected_communes[["admin_id", "parent_commune_id"]].stack()

        return cls(
            countries={"FR"},
            regions=cls.values_without_prefix(selected_communes["region_id"]),
            departements=cls.values_without_prefix(selected_communes["departement_id"]),
            epcis=cls.values_without_prefix(selected_communes["epci_id"]),
            communes=cls.values_without_prefix(commune_ids),
        )

    @staticmethod
    def values_without_prefix(values) -> set[str]:
        """Return raw admin ids, because transport.data.gouv covered_area uses raw ids."""
        values = values.dropna().astype(str)
        return set(values.str.removeprefix("fr-"))

    def matches_dataset(self, dataset: dict) -> bool:
        """Return True when a transport.data.gouv dataset can cover the study area."""
        if self.disabled:
            return True

        covered_area = dataset.get("covered_area")
        if not isinstance(covered_area, list) or len(covered_area) == 0:
            return True

        for area in covered_area:
            if not isinstance(area, dict):
                return True

            area_type = area.get("type")
            area_id = area.get("insee")
            if area_type not in self.known_area_types or area_id is None:
                return True

            if self.matches_area(str(area_type), str(area_id)):
                return True

        return False

    def matches_area(self, area_type: str, area_id: str) -> bool:
        """Return True when one covered_area entry matches the study area."""
        if area_type == "pays":
            return area_id in self.countries
        if area_type == "region":
            return area_id in self.regions
        if area_type == "departement":
            return area_id in self.departements
        if area_type == "epci":
            return area_id in self.epcis
        if area_type == "commune":
            return area_id in self.communes
        return False

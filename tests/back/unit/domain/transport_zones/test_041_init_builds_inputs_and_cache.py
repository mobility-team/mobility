import pathlib

from mobility.transport_zones import TransportZones

def test_init_builds_inputs_and_cache_path(project_dir, fake_inputs_hash, dependency_fakes):
    # Construct with explicit arguments
    local_admin_unit_identifier = "fr-09122"
    level_of_detail = 1
    radius_in_km = 30

    transport_zones = TransportZones(
        local_admin_unit_id=local_admin_unit_identifier,
        level_of_detail=level_of_detail,
        radius=radius_in_km,
    )

    # Verify StudyArea and OSMData were constructed with expected args
    assert len(dependency_fakes.study_area_inits) == 1
    assert dependency_fakes.study_area_inits[0] == {
        "local_admin_unit_id": local_admin_unit_identifier,
        "radius": radius_in_km,
    }

    assert len(dependency_fakes.osm_inits) == 1
    osm_init_record = dependency_fakes.osm_inits[0]
    assert osm_init_record["object_type"] == "a"
    assert osm_init_record["key"] == "building"
    assert osm_init_record["geofabrik_extract_date"] == "240101"
    assert osm_init_record["split_local_admin_units"] is True

    # Cache path must be rewritten to include the hash prefix inside project_dir
    expected_file_name = f"{fake_inputs_hash}-transport_zones.gpkg"
    expected_cache_path = pathlib.Path(project_dir) / expected_file_name
    assert transport_zones.cache_path == expected_cache_path
    assert transport_zones.hash_path == expected_cache_path  # consistency with base asset patch

    # Inputs surfaced as attributes (via patched Asset.__init__)
    assert transport_zones.level_of_detail == level_of_detail
    assert getattr(transport_zones, "study_area") is not None
    assert getattr(transport_zones, "osm_buildings") is not None

    # New instance has no value cached in memory yet
    assert transport_zones.value is None

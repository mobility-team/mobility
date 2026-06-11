from importlib import resources


def test_runtime_resources_needed_by_public_workflows_are_packaged():
    resource_folder = resources.files("mobility.runtime.resources")

    expected_files = [
        "osmdata_0.2.5.005.zip",
        "gtfs/gtfs_route_types.csv",
        "ademe/Base_Carbone_V22.0.csv",
        "ademe/mapping.csv",
        "surveys/entd_mode.xlsx",
    ]

    missing_files = [
        file_path
        for file_path in expected_files
        if not resource_folder.joinpath(file_path).is_file()
    ]

    assert missing_files == []

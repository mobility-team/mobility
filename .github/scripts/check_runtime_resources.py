from importlib import metadata, resources


def main():
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

    if missing_files:
        raise SystemExit(f"Missing files in wheel: {missing_files}")

    print(f"Installed mobility-tools {metadata.version('mobility-tools')} from wheel.")


if __name__ == "__main__":
    main()

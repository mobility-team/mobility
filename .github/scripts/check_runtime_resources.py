from importlib import metadata


def main():
    distribution = metadata.distribution("mobility-tools")
    expected_files = [
        "mobility/runtime/resources/osmdata_0.2.5.005.zip",
        "mobility/runtime/resources/gtfs/gtfs_route_types.csv",
        "mobility/runtime/resources/ademe/Base_Carbone_V22.0.csv",
        "mobility/runtime/resources/ademe/mapping.csv",
        "mobility/runtime/resources/surveys/entd_mode.xlsx",
    ]

    installed_files = {str(file_path).replace("\\", "/") for file_path in distribution.files or []}
    missing_files = [
        file_path
        for file_path in expected_files
        if file_path not in installed_files
    ]

    if missing_files:
        raise SystemExit(f"Missing files in wheel: {missing_files}")

    print(f"Installed mobility-tools {distribution.version} from wheel.")


if __name__ == "__main__":
    main()

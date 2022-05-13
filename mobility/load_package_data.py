#Mobility is free software developed by Elioth (https://elioth.com/) ; you can redistribute it and/or modify it under the terms of the GNU General Public License

import os
import sys
import json
from shutil import copyfile
from distutils.dir_util import copy_tree
from pathlib import Path
import argparse

def load_package_data(package_path, data_path, force):

    if os.path.exists(package_path) and os.path.exists(package_path / "data/package_data.json"):
        with open(package_path / "data/package_data.json", "r") as f:
            package_data = json.load(f)
            package_name = package_data["package_name"]
            package_files = package_data["package_files"]
        for file, destination_path in package_files.items():
            if os.path.exists(package_path / destination_path / file) and force is False:
                print("File", file, "is already in the destination folder (use the --force flag to overwrite it).")
            else:
                if os.path.isfile(data_path / package_name / file):
                    print("Copying file", file, "to", str(package_path / destination_path / file))
                    copyfile(data_path / package_name / file, package_path / destination_path / file)
                elif os.path.isdir(data_path / package_name / file):
                    print("Copying folder", file, "to", str(package_path / destination_path / file))
                    copy_tree(str(data_path / package_name / file), str(package_path / destination_path / file))
                else:
                    raise ValueError("Cannot copy " + str(data_path / package_name / file) + " : not a file nor a folder.")


def main(data_path, force):

    package_path = Path(os.path.abspath(__file__)).parent
    data_path = Path(data_path)

    load_package_data(package_path, data_path, force)

    # Run the load_package_data.py scripts of submodules
    for file_path in package_path.glob('**/load_package_data.py'):
        subpackage_path = Path(os.path.dirname(file_path)).parent
        if subpackage_path != package_path:
            load_package_data(subpackage_path, data_path, force)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_folder_path")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    main(args.data_folder_path, args.force)

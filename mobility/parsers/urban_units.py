import os
import pathlib
import zipfile
import pandas as pd
import numpy as np
from mobility.parsers.download_file import download_file

# Monkey patch openpyxl to avoid an error when opening the urban units INSEE file
# Source : https://stackoverflow.com/questions/71733414/copying-from-a-range-of-cells-with-openpyxl-error-colors-must-be-argb-hex-valu
from openpyxl.styles.colors import WHITE, RGB
__old_rgb_set__ = RGB.__set__
def __rgb_set_fixed__(self, instance, value):
    try:
        __old_rgb_set__(self, instance, value)
    except ValueError as e:
        if e.args[0] == 'Colors must be aRGB hex values':
            __old_rgb_set__(self, instance, WHITE)
RGB.__set__ = __rgb_set_fixed__


def prepare_french_urban_units():
    
    url = "https://www.data.gouv.fr/fr/datasets/r/c59f74bb-8095-4e41-9627-5fecca95668d"
    path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee/UU2020_au_01-01-2023.zip"
    download_file(url, path)

    # Unzip the content
    with zipfile.ZipFile(path, "r") as zip_ref:
        zip_ref.extractall(path.parent)
        
        
def get_french_urban_units():
    
    path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee/UU2020_au_01-01-2023.xlsx"
    
    if path.exists() is False:
        prepare_french_urban_units()
    
    urban_units = pd.read_excel(
        path,
        sheet_name="Composition_communale",
        skiprows=5
    )
    
    urban_units = urban_units.iloc[:, [0, 5]]
    urban_units.columns = ["INSEE_COM", "urban_unit_category"]
    urban_units["urban_unit_category"] = np.where(
        urban_units["urban_unit_category"] != "H",
        urban_units["urban_unit_category"],
        "R"
    )
    
    return urban_units
    
    


    
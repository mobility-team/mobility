import os
import dotenv
import mobility
import pandas as pd

from mobility.parsers import LocalAdminUnits

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

lau = LocalAdminUnits()
x = lau.get()

import os
import dotenv
import mobility

from mobility.parsers import MobilitySurvey

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path="D:/data/mobility/projects/lyon"
)

ms_2019 = MobilitySurvey(source="EMP-2019")
ms_2008 = MobilitySurvey(source="ENTD-2008")
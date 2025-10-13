import os
import sys
import pathlib
import logging
import platform
import json

from importlib import resources
from mobility.r_utils.r_script import RScript


def set_params(
    package_data_folder_path=None,
    project_data_folder_path=None,
    path_to_pem_file=None,
    http_proxy_url=None,
    https_proxy_url=None,
    r_packages=True,
    r_packages_force_reinstall=False,
    r_packages_download_method="auto",
    debug=False
):
    """
    Sets up the necessary environment for the Mobility package.

    This function configures logging, sets various environment variables, and establishes default paths 
    for package and project data folders.

    Parameters:
    package_data_folder_path (str, optional): The file path for storing common datasets used by all projects.
    project_data_folder_path (str, optional): The file path for storing project-specific datasets.
    path_to_pem_file (str, optional): The file path to the PEM file for SSL certification.
    http_proxy_url (str, optional): The URL for the HTTP proxy.
    https_proxy_url (str, optional): The URL for the HTTPS proxy.
    r_packages (boolean, optional): whether to install R packages or not by running RScript (does not work for github actions so is handled by a separate r-lib github action)
    r_packages_force_reinstall (bool, optional)
    r_packages_download_method (str, optional): set this parameter to "wininet" to be able to install packages on some proxies. See the installation.md page for details.
    debug (bool, optional): set debug to True to see the R logs, including error messages
    """

    setup_logging()
    
    set_env_variable("MOBILITY_ENV_PATH", str(pathlib.Path(sys.executable).parent))
    set_env_variable("MOBILITY_CERT_FILE", path_to_pem_file)
    set_env_variable("HTTP_PROXY", http_proxy_url)
    set_env_variable("HTTPS_PROXY", https_proxy_url)
    
    os.environ["MOBILITY_DEBUG"] = "1" if debug else "0"

    setup_package_data_folder_path(package_data_folder_path)
    setup_project_data_folder_path(project_data_folder_path)

    install_r_packages(r_packages, r_packages_force_reinstall, r_packages_download_method)


def set_env_variable(key, value):
    """
    Sets an environment variable.

    Parameters:
    key (str): The name of the environment variable.
    value (str): The value to be set for the environment variable.
    """
    if value is not None:
        os.environ[key] = value


def setup_logging():
    """
    Configures the logging for the Mobility package.

    This function sets up basic logging configuration including format, level, and date format.
    """
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def setup_package_data_folder_path(package_data_folder_path):
    """
    Sets up the package data folder path.

    If a path is provided, it is used; otherwise, a default path is set. This function also ensures 
    the creation of the default folder if it doesn't exist, after user confirmation.

    Parameters:
    package_data_folder_path (str, optional): The file path for storing common datasets.
    """

    if package_data_folder_path is not None:
        
        if not pathlib.Path(package_data_folder_path).exists():
            os.makedirs(package_data_folder_path)
            
        os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] = str(package_data_folder_path)

    else:
        default_path = pathlib.Path.home() / ".mobility/data"
        os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] = str(default_path)

        if default_path.exists() is False:
            logging.info("Mobility needs a folder to store common datasets, that will be used for every project.")
            logging.info("You did not provide the package_data_folder_path argument, so we'll use a default folder : " + str(default_path))

            inp = input("Is this location OK for you ? Yes / No\n")
            inp = inp.lower()

            if "y" in inp:
                os.makedirs(default_path)
            else:
                raise ValueError("Please re run setup_mobility with the package_data_folder_path pointed to your desired location.")


def setup_project_data_folder_path(project_data_folder_path):
    """
    Sets up the project data folder path.

    If a path is provided, it is used; otherwise, a default path is set. This function also ensures 
    the creation of the default folder if it doesn't exist, after user confirmation.

    Parameters:
    project_data_folder_path (str, optional): The file path for storing project-specific datasets.
    """

    if project_data_folder_path is not None:
        
        if not pathlib.Path(project_data_folder_path).exists():
            os.makedirs(project_data_folder_path)
            
        os.environ["MOBILITY_PROJECT_DATA_FOLDER"] = str(project_data_folder_path)

    else:
        default_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "projects"
        os.environ["MOBILITY_PROJECT_DATA_FOLDER"] = str(default_path)

        if default_path.exists() is False:

            logging.info("Mobility needs a folder to cache datasets that are specific to projects.")
            logging.info("You did not provide the project_data_folder_path argument, so we'll use a default folder : " + str(default_path))

            inp = input("Is this location OK for you ? Yes / No\n")
            inp = inp.lower()

            if "y" in inp:
                os.makedirs(default_path)
            else:
                raise ValueError("Please re run setup_mobility with the project_data_folder_path pointed to your desired location.")


def install_r_packages(r_packages, r_packages_force_reinstall, r_packages_download_method):

    if r_packages is True:
    
        packages = [
            {'source': 'CRAN', 'name': 'remotes'},
            {'source': 'CRAN', 'name': 'dodgr'},
            {'source': 'CRAN', 'name': 'sf'},
            {'source': 'CRAN', 'name': 'dplyr'},
            {'source': 'CRAN', 'name': 'sfheaders'},
            {'source': 'CRAN', 'name': 'nngeo'},
            {'source': 'CRAN', 'name': 'data.table'},
            {'source': 'CRAN', 'name': 'arrow'},
            {'source': 'CRAN', 'name': 'hms'},
            {'source': 'CRAN', 'name': 'lubridate'},
            {'source': 'CRAN', 'name': 'future'},
            {'source': 'CRAN', 'name': 'future.apply'},
            {'source': 'CRAN', 'name': 'ggplot2'},
            {'source': 'CRAN', 'name': 'cppRouting'},
            {'source': 'CRAN', 'name': 'duckdb'},
            {'source': 'CRAN', 'name': 'jsonlite'},
            {'source': 'CRAN', 'name': 'gtfsrouter'},
            {'source': 'CRAN', 'name': 'geos'},
            {'source': 'CRAN', 'name': 'FNN'},
            {'source': 'CRAN', 'name': 'cluster'},
            {'source': 'CRAN', 'name': 'dbscan'}
        ]
        
        if platform.system() == "Windows":
            packages.append(
                {
                    "source": "local",
                    "path": str(resources.files('mobility.resources').joinpath('osmdata_0.2.5.005.zip'))
                }
            )
        else:
            packages.append({'source': 'CRAN', 'name': 'osmdata'})
            
            
        args = [
            json.dumps(packages),
            str(r_packages_force_reinstall),
            r_packages_download_method
        ]
            
        script = RScript(resources.files('mobility.r_utils').joinpath('install_packages.R'))
        script.run(args)

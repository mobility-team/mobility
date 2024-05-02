import os
import pathlib
import logging

def setup_mobility(
        package_data_folder_path=None, project_data_folder_path=None,
        path_to_pem_file=None, http_proxy_url=None, https_proxy_url=None
    ):
    
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    if package_data_folder_path is not None:
        
        os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] = package_data_folder_path
        
    else:
        
        default_path = pathlib.Path.home() / ".mobility/data"
        os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] = str(default_path)
        
        if default_path.exists() is False:
            
            print("Mobility needs a folder to store common datasets, that will be used for every project.")
            print("You did not provide the package_data_folder_path argument, so we'll use a default folder : " + str(default_path))
            
            inp = input("Is this location OK for you ? Yes / No\n")
            inp = inp.lower()
            
            if "y" in inp:
                os.makedirs(default_path)
            else:
                raise ValueError("Please re run setup_mobility with the package_data_folder_path pointed to your desired location.")
            
            
    if project_data_folder_path is not None:
        
        os.environ["MOBILITY_PROJECT_DATA_FOLDER"] = project_data_folder_path
        
    else:
        
        default_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "projects"
        
        if default_path.exists() is False:
            
            print("Mobility needs a folder to cache datasets that are specific to projects.")
            print("You did not provide the project_data_folder_path argument, so we'll use a default folder : " + str(default_path))
            
            inp = input("Is this location OK for you ? Yes / No\n")
            inp = inp.lower()
            
            if "y" in inp:
                os.makedirs(default_path)
            else:
                raise ValueError("Please re run setup_mobility with the project_data_folder_path pointed to your desired location.")
            
    
    if path_to_pem_file is not None:
        os.environ["MOBILITY_CERT_FILE"] = path_to_pem_file
    
    if http_proxy_url is not None:
        os.environ["HTTP_PROXY"] = http_proxy_url
    
    if https_proxy_url is not None:
        os.environ["HTTPS_PROXY"] = https_proxy_url
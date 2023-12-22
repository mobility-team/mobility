import os
import subprocess
import pathlib
import logging

from mobility.parsers.osm import prepare_osm

def prepare_dodgr_graph(transport_zones, mode, force=False):
    
    graph_file_name = "dodgr_graph_" + mode + ".rds"
    graph_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / graph_file_name
    
    if graph_file_path.exists() is False or force is True:
    
        logging.info("Creating a routable graph with dodgr, this might take a while...")
        
        dodgr_modes = {
            "car": "motorcar"
        }
        
        logging.info("Preparing OSM data...")
        
        osm_file_path = prepare_osm(transport_zones)
        
        tzs_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones.gpkg"
        transport_zones.to_file(tzs_file_path)
        
        logging.info("Running R script...")
        
        args = [
            "-t", tzs_file_path,
            "-n", osm_file_path,
            "-m", dodgr_modes[mode],
            "-o", graph_file_path
        ]
    
        path_to_r_script = pathlib.Path(__file__).parent / "prepare_dodgr_graph.R"
        
        cmd = ["Rscript", path_to_r_script] + args
        
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
        while True:
            output = process.stdout.readline()
            
            if output:
                print(output.strip())
        
            if process.poll() is not None:
                break
        
        # Check if there are any remaining messages/errors after the process ends
        for output in process.stdout.readlines():
            print(output.strip())
                
    else:
        
        logging.info("Dodgr graph already prepared. Reusing the file : " + str(graph_file_path))

    
    return graph_file_path

    
    
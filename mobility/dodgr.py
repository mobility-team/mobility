import os
import subprocess
import pathlib
import logging
import threading
import pandas as pd

from mobility.dodgr_modes import get_dodgr_mode
from mobility.parsers.osm import prepare_osm
from mobility.caching import is_update_needed

def compute_travel_costs(transport_zones, mode):
    
    inputs = {
        "transport_zones": transport_zones.inputs_hash,
        "mode": mode
    }
    
    output_file_name = "dodgr_travel_costs_" + mode + ".parquet"
    output_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / output_file_name
    
    update_needed, inputs_hash = is_update_needed(inputs, output_file_path)
    
    osm = prepare_osm(transport_zones, mode, update_needed)
    graph = dodgr_graph(transport_zones, osm, mode, update_needed)
    costs = dodgr_costs(transport_zones, mode, graph, output_file_path, update_needed)
    
    return costs


def dodgr_graph(transport_zones, osm, mode, update_needed=True):
    
    dodgr_mode = get_dodgr_mode(mode)
    
    output_file_name = "dodgr_graph_" + dodgr_mode + ".rds"
    output_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / output_file_name
    
    if update_needed is True:
    
        logging.info("Creating a routable graph with dodgr, this might take a while...")
        
        tzs_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones.gpkg"
        
        args = [
            "-t", tzs_file_path,
            "-n", osm,
            "-m", dodgr_mode,
            "-o", output_file_path
        ]
    
        r_script_path = pathlib.Path(__file__).parent / "prepare_dodgr_graph.R"
        
        run_r_script(r_script_path, args)
                        
    else:
        
        logging.info("Dodgr graph already prepared. Reusing the file : " + str(output_file_path))

    
    return output_file_path


def dodgr_costs(transport_zones, mode, graph, output_file_path, update_needed=True):
    
    if update_needed is True:
    
        logging.info("Computing travel costs...")
        
        tzs_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones.gpkg"
        
        args = [
            "-t", tzs_file_path,
            "-g", graph,
            "-o", output_file_path
        ]
    
        r_script_path = pathlib.Path(__file__).parent / "prepare_travel_costs.R"
        
        run_r_script(r_script_path, args)
                        
    else:
        
        logging.info("Travel costs already prepared. Reusing the file : " + str(output_file_path))
        
    
    costs = pd.read_parquet(output_file_path)

    return costs


def run_r_script(r_script_path, args):
        
    cmd = ["Rscript", r_script_path] + args
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    output_thread = threading.Thread(target=print_output, args=(process.stdout,))
    output_thread.start()
    
    process.wait()
    
    output_thread.join()



def print_output(stream):
    for line in iter(stream.readline, b''):
        msg = line.decode()
        if "INFO" in msg:
            msg = msg.split("]")[1]
            msg = msg.strip()
            logging.info(msg)
    
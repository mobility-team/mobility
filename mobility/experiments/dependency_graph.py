import os
import dotenv
import mobility
import pandas as pd

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"],
    debug=False,
    r_packages_force_reinstall=False
)

# Prepare transport zones
transport_zones = mobility.TransportZones("fr-24037", radius=21.0)


# -----------------------------------------------------------------------------
# Model for the year 2024 
multi_modal_mode_2024 = mobility.MultiModalMode(transport_zones)
df = multi_modal_mode_2024.travel_costs.get()


from mobility.asset import Asset

def build_dependency_graph(asset):
    
    nodes = {}
    edges = []
    
    def walk(out):
        
        out_id = id(out)
    
        if out_id not in nodes.keys():
            nodes[out_id] = out
        
        if isinstance(out, Asset):
            for k, inp in out.inputs.items():
                
                inp_id = id(inp)                
                edges.append([inp_id, out_id])
                
                if hasattr(inp, "inputs") and inp_id not in nodes.keys():
                    walk(inp)
                else:
                    nodes[id(inp)] = inp
            
    walk(asset)
    
    return nodes, edges
    
    
nodes, edges = build_dependency_graph(multi_modal_mode_2024.travel_costs)



import dash
import dash_cytoscape as cyto
from dash import html

cyto.load_extra_layouts()

# Create a Dash app
app = dash.Dash(__name__)


elements = [{"data": {"id": str(k), "label": str(v)}} for k, v in nodes.items()]
elements += [{"data": {"source": str(edge[0]), "target": str(edge[1])}} for edge in edges]

# Define the layout and styling of the network
app.layout = html.Div([
    cyto.Cytoscape(
        id='cytoscape-network',
        elements=elements,
        layout={'name': 'cose', "nodeRepulsion": 2048*4, "idealEdgeLength": 64, "edgeElasticity": 128, "gravity": 0.5, "nodeOverlap": 128},
        style={'width': '100%', 'height': '600px'},
        stylesheet=[
            {
                'selector': 'node',
                'style': {
                    "shape": "rectangle",
                    'content': 'data(label)',
                    'background-color': '#0074D9',
                    'color': 'black',
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'font-size': '12px',
                    'width': '50px',
                    'height': '50px'
                }
            },
            {
                'selector': 'edge',
                'style': {
                    'width': 2,
                    'line-color': '#A3C4BC',
                    'target-arrow-color': '#A3C4BC',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier'
                }
            }
        ]
    )
])

# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)
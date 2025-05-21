import pathlib
import geopandas as gpd
import dash_leaflet as dl
import dash_leaflet.express as dlx
import dash
import sqlite3
import json
import dash_bootstrap_components as dbc
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.colors as colors

from mapclassify import classify

from dash_extensions.javascript import assign

app = dash.Dash(
    external_stylesheets=[dbc.themes.BOOTSTRAP]
)

app.layout = dash.html.Div(
    children=[
        dash.dcc.Store("paths-store"),
        dbc.Row(
            children=[
                dbc.Col(
                    width=3,
                    style={"padding": "2em"},
                    children=[
                        dash.html.H1("Mobility"),
                        dash.html.Hr(),
                        dbc.Row(
                            children=dbc.Col(
                                children=[
                                    dash.html.H5("Set up"),
                                ]
                            )
                        ),
                        dbc.Row(
                            style={"margin-bottom": "1em"},
                            children=dbc.Col(
                                children=[
                                    dbc.Label("Project folder"),
                                    dbc.Input(
                                        id="project-path-input"
                                    )
                                ]
                            )
                        ),
                        dbc.Row(
                            dbc.Col(
                                children=dbc.Button(
                                    id="update-button",
                                    outline=True,
                                    color="primary",
                                    children="Update data links"
                                )
                            )
                        ),
                        dash.html.Hr(),
                        dbc.Row(
                            children=dbc.Col(
                                children=[
                                    dash.html.H5("Transport zones"),
                                ]
                            )
                        ),
                        dbc.Row(
                            style={"margin-bottom": "1em"},
                            children=dbc.Col(
                                children=[
                                    dbc.Label("Version"),
                                    dash.dcc.Dropdown(
                                        id="transport-zones-version-dropdown"
                                    )
                                ]
                            )
                        ),
                        dbc.Row(
                            dbc.Col(
                                children=dbc.Button(
                                    id="visualize-transport-zones-button",
                                    outline=True,
                                    color="primary",
                                    children="Visualize"
                                )
                            )
                        ),
                        dash.html.Hr(),
                        dbc.Row(
                            children=dbc.Col(
                                children=[
                                    dash.html.H5("Transport costs"),
                                ]
                            )
                        ),
                        dbc.Row(
                            style={"margin-bottom": "1em"},
                            children=dbc.Col(
                                children=[
                                    dbc.Label("Mode"),
                                    dash.dcc.Dropdown(
                                        id="transport-costs-modes-dropdown"
                                    )
                                ]
                            )
                        ),
                        dbc.Row(
                            style={"margin-bottom": "1em"},
                            children=dbc.Col(
                                children=[
                                    dbc.Label("Version"),
                                    dash.dcc.Dropdown(
                                        id="transport-costs-version-dropdown"
                                    )
                                ]
                            )
                        ),
                        dbc.Row(
                            style={"margin-bottom": "1em"},
                            children=dbc.Col(
                                children=[
                                    dbc.Label("Origin (transport zone id)"),
                                    dash.dcc.Dropdown(
                                        id="transport-costs-origin-dropdown"
                                    )
                                ]
                            )
                        ),
                        dbc.Row(
                            dbc.Col(
                                children=dbc.Button(
                                    id="visualize-transport-costs-button",
                                    outline=True,
                                    color="primary",
                                    children="Visualize"
                                )
                            )
                        ),
                    ]
                ),
                dbc.Col(
                    children=dl.Map(
                        id="map",
                        children=dl.TileLayer(),
                        center=[46.5545527, 4.4508261], zoom=7,
                        style={'height': '100vh'}
                    )
                )
            ]
        )
        
        
    ]
)


@app.callback(
    dash.Output("paths-store", "data"),
    dash.Input("update-button", "n_clicks"),
    dash.State("project-path-input", "value"),
    prevent_initial_call=True
)
def update_file_paths(n, project_path):
    
    if project_path is None:
        raise dash.exceptions.PreventUpdate
    
    db_path = pathlib.Path(project_path) / "ui.sqlite"
    
    if db_path.exists() is False:
        raise dash.exceptions.PreventUpdate
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT file_name, inputs_hash, cache_path FROM ui_cache")
        rows = cursor.fetchall()
        
    ui_db_dict = {}
        
    for file_name, inputs_hash, cache_path in rows:
        if file_name not in ui_db_dict:
            ui_db_dict[file_name] = {}
        ui_db_dict[file_name][inputs_hash] = cache_path
    
    return ui_db_dict



@app.callback(
    dash.Output("transport-zones-version-dropdown", "options"),
    dash.Output("transport-costs-modes-dropdown", "options"),
    dash.Input("paths-store", "data"),
    prevent_initial_call=True
)
def update_options(data):
    
    tz_options = [{"value": cache_path, "label": inputs_hash} for inputs_hash, cache_path in data["transport_zones.gpkg"].items()]
    
    tc_keys = [k for k in data.keys() if "travel_costs_" in k]
    tc_keys = list(set(tc_keys))
    tc_keys = [{"value": k, "label": k.replace("travel_costs_", "").replace(".parquet", "")} for k in tc_keys]
    
    return tz_options, tc_keys
    


@app.callback(
    dash.Output("transport-costs-version-dropdown", "options"),
    dash.Input("transport-costs-modes-dropdown", "value"),
    dash.State("paths-store", "data"),
    prevent_initial_call=True
)
def update_transport_costs_version_options(mode, data):
    
    options = [{"value": cache_path, "label": inputs_hash} for inputs_hash, cache_path in data[mode].items()]
    
    return options
    

@app.callback(
    dash.Output("transport-costs-origin-dropdown", "options"),
    dash.Input("transport-costs-version-dropdown", "value"),
    prevent_initial_call=True
)
def update_transport_costs_origin_options(path):
    
    if path is not None:
    
        tc = pd.read_parquet(path)
        options = [{"value": tz_id, "label": tz_id} for tz_id in tc["from"].unique()]
        
    else:
        
        options = []
    
    return options

    


# Function to generate color scale from colormap
def get_color_scale(colormap_name, num_classes):
    cmap = plt.get_cmap(colormap_name, num_classes)
    return [colors.rgb2hex(cmap(i)) for i in range(cmap.N)]

colorscale = get_color_scale("magma", 9)
colorscale.reverse()

style_fun = assign(
    """
        function(feature, context){
        
            const {classes, colorscale, style, colorProp} = context.hideout || {};  
            const value = feature.properties[colorProp];
        
            style.fillColor = 'gray';
            
            if (value !== undefined && value !== null) {
                for (let i = 0; i < classes.length-1; ++i) {
                    if (value > classes[i] && value <= classes[i+1]) {
                        style.fillColor = colorscale[i];
                        break;
                    }
                }
            }
            
            return style;
        
        }
    """
)
    
    
@app.callback(
    dash.Output("map", "children"),
    dash.Input("visualize-transport-zones-button", "n_clicks"),
    dash.Input("visualize-transport-costs-button", "n_clicks"),
    dash.State("transport-zones-version-dropdown", "value"),
    dash.State("transport-costs-version-dropdown", "value"),
    dash.State("transport-costs-origin-dropdown", "value"),
    prevent_initial_call=True
)
def show_transport_zones(n1, n2, tz_path, tc_path, origin):
    
    transport_zones_path = pathlib.Path(tz_path)    
    transport_zones = gpd.read_file(transport_zones_path)
    
    hideout = None
    style_handle = None
    
    if tc_path is not None and origin is not None:
    
        travel_costs = pd.read_parquet(tc_path)
          
        transport_zones = pd.merge(
            transport_zones,
            travel_costs[travel_costs["from"] == origin],
            left_on="transport_zone_id",
            right_on="to",
            how="left"
        )
        
        classifier = classify(transport_zones["time"].dropna(), scheme="quantiles", k=8)
        classes = classifier.bins.tolist()
        
        classes = [0.0] + classes + [1000.0]

        style = dict(weight=2, opacity=1, color='white', fillOpacity=0.7)
        
        hideout = dict(
            colorscale=colorscale,
            classes=classes,
            style=style,
            colorProp="time"
        )    
        
        style_handle = style_fun
        
    transport_zones_gbf = json.loads(transport_zones.to_json(to_wgs84=True))
    transport_zones_gbf = dlx.geojson_to_geobuf(transport_zones_gbf)
    
    children = [
        dl.TileLayer(),
        dl.GeoJSON(
            data=transport_zones_gbf,
            format="geobuf",
            zoomToBounds=True,
            style=style_handle,
            hideout=hideout
        )
    ]
    
    return children
        

            
        

if __name__ == '__main__':
    
    app.run(debug=True)
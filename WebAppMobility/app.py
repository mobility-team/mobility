import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import plotly.graph_objects as go
import layout  # Importation du fichier de layout

# Initialisation de l'application Dash avec suppression des erreurs pour les composants dynamiques
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Définition du layout de l'application
app.layout = layout.create_layout()

# Callback UNIQUE pour gérer la boîte "Zone d'étude" et les ajouts dynamiques
@app.callback(
    [Output("commune_list", "data"), 
     Output("departement_list", "data"), 
     Output("dynamic_zone_input", "children")],
    [Input("study_area", "value"), 
     Input({"type": "add_button", "index": ALL}, "n_clicks")],
    [State("commune_list", "data"), 
     State("departement_list", "data")]
)
def update_study_area_inputs(study_area, n_clicks_list, commune_list, departement_list):
    ctx = dash.callback_context  # Vérifier quelle action a déclenché le callback
    
    if not ctx.triggered:
        # 🔥 Forcer l'affichage des champs pour "rayon" au démarrage
        if study_area == "rayon":
            return commune_list, departement_list, html.Div([
                html.Label("Commune d'origine"),
                dcc.Input(id="commune_origin", type="text", value=""),
                html.Label("Rayon (en km)"),
                dcc.Input(id="rayon_km", type="number", value=0)
            ])
        else:
            return commune_list, departement_list, dash.no_update
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]  # ID de l'élément déclencheur

    # Ajouter une commune
    if "add_commune" in button_id and study_area == "communes":
        commune_list.append("")

    # Ajouter un département
    elif "add_departement" in button_id and study_area == "departements":
        departement_list.append("")

    # Mise à jour dynamique de la section "Zone d'étude"
    if study_area == "rayon":
        zone_input = html.Div([
            html.Label("Commune d'origine"),
            dcc.Input(id="commune_origin", type="text", value=""),
            html.Label("Rayon (en km)"),
            dcc.Input(id="rayon_km", type="number", value=0)
        ])
    
    elif study_area == "communes":
        zone_input = html.Div([
            html.Label("Liste de communes (Format code_de_pays-code_de_ville)"),
            *[dcc.Input(id={"type": "commune_input", "index": i}, type="text", value=commune) for i, commune in enumerate(commune_list)],
            html.Button("+", id={"type": "add_button", "index": "add_commune"}, n_clicks=0)
        ])
    
    elif study_area == "departements":
        zone_input = html.Div([
            html.Label("Département"),
            *[dcc.Input(id={"type": "departement_input", "index": i}, type="text", value=dep) for i, dep in enumerate(departement_list)],
            html.Button("+", id={"type": "add_button", "index": "add_departement"}, n_clicks=0)
        ])

    return commune_list, departement_list, zone_input

# Fonction pour créer un graphique dynamique
def create_dynamic_graph(dropdown_value, transport_value, study_value, socio_value):
    x_values = [1, 2, 3, 4, 5]
    y_values = [10 + i for i in range(5)]
    fig = go.Figure(data=go.Scatter(x=x_values, y=y_values, mode='lines+markers', name=str(dropdown_value)))
    fig.update_layout(title=dropdown_value, xaxis_title="X", yaxis_title="Y")
    return fig

# Callback principal pour générer le graphique
@app.callback(
    Output("output", "children"),
    [Input("run_simulation", "n_clicks")],
    [State("dropdown", "value"),
     State("transport_mode", "value"),
     State("study_area", "value"),
     State("socio_category", "value")]
)
def update_output(n_clicks, dropdown_value, transport_value, study_value, socio_value):
    fig = create_dynamic_graph(dropdown_value, transport_value, study_value, socio_value)
    return dcc.Graph(figure=fig)

if __name__ == "__main__":
    app.run_server(debug=True)

from dash import dcc, html

def create_layout():
    return html.Div([
        # Menu déroulant
        html.Div([
            dcc.Dropdown(
                id="dropdown",
                options=[
                    {"label": "Probabilité de destination", "value": "destination"},
                    {"label": "Probabilité de mode de transport", "value": "transport"}
                ],
                value="destination"
            )
        ], style={'width': '50%', 'margin': '10px'}),

        # Boîte "Mode de transport"
        html.Div([
            html.Label("Mode de transport"),
            dcc.Checklist(
                id="transport_mode",
                options=[
                    {"label": "Vélo", "value": "vélo"},
                    {"label": "Marche", "value": "marche"},
                    {"label": "Voiture", "value": "voiture"},
                    {"label": "TEC", "value": "tec"},
                    {"label": "Avion", "value": "avion"},
                    {"label": "Bateau", "value": "bateau"}
                ],
                value=["vélo", "marche"]
            )
        ], style={'margin': '10px'}),

        # Boîte "Zone d'étude"
        html.Div([
            html.Label("Zone d'étude"),
            dcc.RadioItems(
                id="study_area",
                options=[
                    {"label": "Rayon", "value": "rayon"},
                    {"label": "Communes", "value": "communes"},
                    {"label": "Départements", "value": "departements"}
                ],
                value="rayon"
            ),
            html.Div([
                html.Label("Zone 1"),
                dcc.Input(id="text_zone1", type="text", value=""),
                html.Label("Zone 2"),
                dcc.Input(id="text_zone2", type="text", value="")
            ], style={'margin': '10px'})
        ], style={'margin': '10px'}),

        # Boîte "Catégorie socio-professionnelle"
        html.Div([
            html.Label("Catégorie socio-professionnelle"),
            dcc.Checklist(
                id="socio_category",
                options=[
                    {"label": "Agriculteurs", "value": "agriculteurs"},
                    {"label": "Artisans", "value": "artisans"},
                    {"label": "Employés", "value": "employés"},
                    {"label": "Retraités", "value": "retraités"},
                    {"label": "Cadres et prof. int. sup.", "value": "cadres_sup"},
                    {"label": "Prof. intermédiaires", "value": "prof_intermediaires"},
                    {"label": "Inactifs", "value": "inactifs"}
                ],
                value=["employés"]
            )
        ], style={'margin': '10px'}),

        # Bouton "Lancer la simulation"
        html.Div([
            html.Button("Lancer la simulation", id="run_simulation")
        ], style={'margin': '10px'}),

        # Affichage des résultats dans la console
        html.Div(id="output", style={'margin': '10px'})
    ])

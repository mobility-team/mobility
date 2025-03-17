import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import layout  # Importation du fichier de layout

# Initialisation de l'application Dash
app = dash.Dash(__name__)

# Définition du layout de l'application
app.layout = layout.create_layout()

# Fonction pour créer un graphique dynamique
def create_dynamic_graph(dropdown_value, transport_value, study_value, socio_value, zone1, zone2):
    # Par exemple, générez des données basées sur les choix de l'utilisateur
    x_values = [1, 2, 3, 4, 5]  # Ces données peuvent changer selon les sélections
    y_values = [10 + i for i in range(5)]  # Idem pour les valeurs en Y

    # Créer un graphique dynamique avec Plotly
    fig = go.Figure(data=go.Scatter(x=x_values, y=y_values, mode='lines+markers', name = str(dropdown_value)))

    # Ajouter un titre et des labels
    fig.update_layout(
        title= dropdown_value,
        xaxis_title="X",
        yaxis_title="Y"
    )
    return fig


# Définition des callbacks pour interagir avec les éléments
@app.callback(
    Output("output", "children"),
    [Input("run_simulation", "n_clicks")],
    [State("dropdown", "value"),
     State("transport_mode", "value"),
     State("study_area", "value"),
     State("socio_category", "value"),
     State("text_zone1", "value"),
     State("text_zone2", "value")]
)
def update_output(n_clicks, dropdown_value, transport_value, study_value, socio_value, zone1, zone2):
    # Si le bouton n'a pas été cliqué, ne rien afficher
    if n_clicks is None:
        return ""

    # Créer le graphique dynamique avec les données
    fig = create_dynamic_graph(dropdown_value, transport_value, study_value, socio_value, zone1, zone2)

    # Retourner le graphique Plotly pour l'affichage
    return dcc.Graph(figure=fig)

if __name__ == "__main__":
    app.run_server(debug=True)

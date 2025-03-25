from dash import Dash, html, Input, Output, State, callback, dcc, Patch
import json


with open('interface_content.json', 'r') as file:
    app_content = json.load(file)


app = Dash()

app.layout = html.Div(
    [
         html.Div(
             [
                 html.H2('Simulation choisie', className='title_box'),
                 dcc.Dropdown(["Probabilité de destination", "Choix B ?"], "Probabilité de destination", 
                              id="sim_dropdown", 
                              searchable=False,
                              clearable=False)
             ], className='div_box'),
         
         html.Div(
             [
                 html.H2('Modes de transports', className='title_box'),
                 dcc.Checklist(['Marche', 'Vélo', 'Avion', 'Voiture', 'Transport en commun'],
                               [], 
                               id='transport_mode')
             ], className='div_box'),
         
         html.Div(
             [
                 html.H2("Zone d'étude", className='title_box'),
                 dcc.Tabs(id="tabs_study_zone", value='tab-commune', children=[
                     dcc.Tab(label='Rayon', value='tab-rayon'),
                     dcc.Tab(label='Commune', value='tab-commune'),
                     dcc.Tab(label='Départements', value='tab-departement')
                 ]),
                 html.Div(id="study_zone_content")
             ], className='div_box'),

         html.Div(
             [
                 html.H2("Catégories socio-professionnelles", className='title_box'),
                 dcc.Checklist(['Agriculteur', 'Artisants', 'Ouvriers'], 
                               [],
                               id='csp_checkbox')
             ], className='div_box'),

         html.Button('Lancer la simulation', 
                     id='sim_button'),
         
         html.Button('Télécharger le CSV', 
                     id='dl_CSV'),
         
         html.Button('Télécharger le SVF', 
                     id='dl_SVF')
    ])


@callback(Output("study_zone_content", "children"),
          Input("tabs_study_zone", "value"))

def update_study_zone(value):
    if value == "Rayon":
        new_input_1 = dcc.Input(id='input_rayon_commune')
        new_input_2 = dcc.Input(id='input_rayon_value')
        
        return html.Div([dcc.Input(id='input_rayon_commune'), dcc.Input(id='input_rayon_value')])
        














if __name__ == '__main__':
    app.run(debug=True)
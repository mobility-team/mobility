from dash import Dash, html, Input, Output, State, callback, dcc, Patch, ALL
import dash_bootstrap_components as dbc
import json


with open('interface_content.json', 'r') as file:
    app_content = json.load(file)


app = Dash(suppress_callback_exceptions=True)

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
                 dcc.Tabs(id="tabs_study_zone", value='tab-rayon', children=[
                     dcc.Tab(label='Rayon', value='tab-rayon', className="zone_tab"),
                     dcc.Tab(label='Commune', value='tab-municipality', className="zone_tab"),
                     dcc.Tab(label='Départements', value='tab-county', className="zone_tab")
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
                     id='dl_SVF'),
         
         html.Button('Paramètres',
                     id='settings'),
         
         html.P(id='text')
    ], id="global_container")



@callback(Output("study_zone_content", "children"),
          Input("tabs_study_zone", "value"))

def update_study_zone(value):
    if value == "tab-rayon":
        text_input_1 = html.P("Commune d'origine")
        text_input_2 = html.P("Rayon de recherche")
        new_input_1 = dcc.Input(id='input_radius_municipality', className='zone_input')
        new_input_2 = dcc.Input(id='input_radius_value', className='zone_input')
        
        return html.Div([text_input_1, new_input_1, text_input_2, new_input_2])
        

    elif value == "tab-municipality":        
        text_input_1 = html.P("Commune d'origine")
        text_input_2 = html.P("Liste de communes")
        new_input_1 = dcc.Input(id='input_municipality_value')
        new_div_municipality = html.Div(id="div_container_municipality", children=[])
        button = html.Button("+", id="add_input_municipality", className='zone_button')
        
        return html.Div([text_input_1, new_input_1, text_input_2, new_div_municipality, button])
        
    
    elif value == "tab-county":        
        text_input = html.P("Liste de départements")
        new_div_county = html.Div(id="div_container_county", children=[])
        button = html.Button("+", id="add_input_county", className='zone_button')
        
        return html.Div([text_input, new_div_county, button])




@callback(Output("div_container_municipality", "children"),
          Input("add_input_municipality", "n_clicks"))


def add_municipality_input(n_clicks):
    patched_children = Patch()
    new_input = dcc.Input(id={"type": "input_municipality", "index": n_clicks})
    
    patched_children.append(new_input)
    return patched_children



@callback(Output("div_container_county", "children"),
          Input("add_input_county", "n_clicks"))


def add_county_input(n_clicks):
    patched_children = Patch()
    new_input = dcc.Input(id={"type": "input_county", "index": n_clicks})
    
    patched_children.append(new_input)
    return patched_children


@callback(Output("text", "children"),
          Input("sim_button", "n_clicks"),
          State("tabs_study_zone", "value"),
          
          State("input_radius_municipality", "value"),
          State("input_radius_value", "value"),
          
          State({"type": "input_county", "index": ALL}, "value"),
          
          State({"type": "input_municipality", "index": ALL}, "value"),
          State("input_municipality_value", "value")
          
)
          


def start_sim(n_clicks, current_tab, input_radius_municipality, input_radius_value, input_county, input_municipality, value_input_municipality):
    if current_tab == "tab-rayon" :
        return f"Ville d'origine choisie {input_radius_municipality}"
    
    if current_tab == "tab-municipality":
        pass
    
    if current_tab == "tab-county":
        pass





if __name__ == '__main__':
    app.run(debug=True)
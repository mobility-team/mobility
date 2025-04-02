"""
Ce fichier contient la fonction Layout qui permet de crée l'interface utilisateur
et remplace html
"""

from dash import html, dcc
import dash_bootstrap_components as dbc
import json
import pandas as pd

#Importation des fichiers json pour les contenus et la traduction
with open('interface_content.json', 'r') as file:
    app_content = json.load(file)
    
with open('translations_content.json', 'r') as file:
    app_translation = json.load(file)




def Layout(language, image):
    """
    Création de l'interface utilisateur en fonction de la langue
    
    Paramètres :
        language(str) : Code de la langue (ex : 'fr')
        image(str) : lien de l'image. Si le lien n'est pas chargé à l'avance, l'image
                    est innacessible
    """
    
    #Le Div contenant tout l'application
    return[   
             html.Div(#Div contenant le logo mobility
                [    #Image avec le logo mobility
                     html.Img(src=image, 
                             id=app_content['box mobility']['logo']['id']),
                     
                     #Bouton pour l'infobulle
                     html.Button(children="?",
                                 className=app_content['box mobility']['button infobulle1']['class'],
                                 id=app_content['box mobility']['button infobulle1']['id'],
                                 disabled=True),
                     
                     #Infobulle
                     dbc.Tooltip(children=app_translation[app_content['box mobility']['button infobulle1']['infobulle1']['label']][language],
                                 target=app_content['box mobility']['button infobulle1']['id'],
                                 className=app_content['box mobility']['button infobulle1']['infobulle1']['class'], 
                                 id=app_content['box mobility']['button infobulle1']['infobulle1']['id'])
             ], className=app_content['div_box']['class']),
             
             html.Div( #Div contenant le menu déroulant du mode de simulation
                 [   #Titre de la boîte
                     html.H2(children=app_translation[app_content['div_box']['box simulation']['title']['label']][language], 
                             className=app_content['div_box']['box simulation']['title']['class'],
                             id=app_content['div_box']['box simulation']['title']['id']),
                     
                     #Bouton pour l'infobulle
                     html.Button("?", 
                                 className=app_content['div_box']['box simulation']['button infobulle2']['class'], 
                                 id=app_content['div_box']['box simulation']['button infobulle2']['id'], 
                                 disabled=True),
                     
                     #Infobulle
                     dbc.Tooltip(children=app_translation[app_content['div_box']['box simulation']['button infobulle2']['infobulle2']['label']][language], 
                                 target=app_content['div_box']['box simulation']['button infobulle2']['id'], 
                                 id=app_content['div_box']['box simulation']['button infobulle2']['infobulle2']['id'],
                                 className=app_content['div_box']['box simulation']['button infobulle2']['infobulle2']['class']),
                     
                     #Menu déroulant des modes de simulation
                     dcc.Dropdown(options=[app_translation[o][language] for o in app_content['div_box']['box simulation']['dropdown']['options']],
                                  value=app_translation[app_content['div_box']['box simulation']['dropdown']['options'][0]]['fr'], 
                                  id=app_content['div_box']['box simulation']['dropdown']['id'], 
                                  searchable=False,
                                  clearable=False)
                 ], className=app_content['div_box']['class']),
             
             html.Div( #Div contenant le choix du mode de transport 
                 [   #Titre de la boîte
                     html.H2(app_translation[app_content['div_box']['box means transport']['title']['label']][language], 
                             className=app_content['div_box']['box means transport']['title']['class'],
                             id=app_content['div_box']['box means transport']['title']['id']),
                     
                     #Button pour l'infobulle
                     html.Button("?",
                                 className=app_content['div_box']['box means transport']['button infobulle3']['class'], 
                                 id=app_content['div_box']['box means transport']['button infobulle3']['id'],
                                 disabled=True
                                 ),
                     #Infobulle
                     dbc.Tooltip(children=app_translation[app_content['div_box']['box means transport']['button infobulle3']['infobulle3']['label']][language], 
                                 target=app_content['div_box']['box means transport']['button infobulle3']['id'],
                                 id=app_content['div_box']['box means transport']['button infobulle3']['infobulle3']['id'], 
                                 className=app_content['div_box']['box means transport']['button infobulle3']['infobulle3']['class']
                                 ),
                     
                     #Liste de cases à cocher pour les moyens de transports
                     dcc.Checklist(options=[app_translation[o][language] for o in app_content['div_box']['box means transport']['transport_means']['options']],
                                   value=[app_translation[o][language] for o in app_content['div_box']['box means transport']['transport_means']['options']], 
                                   id=app_content['div_box']['box means transport']['transport_means']['id'])
                 ], className=app_content['div_box']['class']),
             
             html.Div( #Div contenant le choix de la zone d'étude
                 [   #Titre de la boîte
                     html.H2(app_translation[app_content['div_box']['box study area']['title']['label']][language],
                             className=app_content['div_box']['box study area']['title']['class'],
                             id=app_content['div_box']['box study area']['title']['id']),
                     
                     # Bouton pour l'infobulle
                     html.Button("?", 
                                 className=app_content['div_box']['box study area']['button infobulle4']['class'], 
                                 id=app_content['div_box']['box study area']['button infobulle4']['id'], 
                                 disabled=True),
                     
                     #Infobulle
                     dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['button infobulle4']['infobulle4']['label']][language], 
                                 target=app_content['div_box']['box study area']['button infobulle4']['id'], 
                                 id=app_content['div_box']['box study area']['button infobulle4']['infobulle4']['id'], 
                                 className=app_content['div_box']['box study area']['button infobulle4']['infobulle4']['class']),
                     
                     #Onglets pour le choix de la zone
                     dcc.Tabs(id="tabs_study_zone", 
                              value='tab-rayon', 
                              children=[
                                     #Onlget pour la calcul par rayon
                                     dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['radius']['title']['label']][language], 
                                             value='tab-rayon', 
                                             className=app_content['div_box']['box study area']['choice']['class'], 
                                             children = [#Texte pour l'input 
                                                         html.P(app_translation[app_content['div_box']['box study area']['choice']['radius']['municipality origin txt']['label']][language]),                                                     
                                                         
                                                         #Bouton pour l'infobulle
                                                         html.Button("?", 
                                                                     className=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['class'], 
                                                                     id=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['id'], 
                                                                     disabled=True),
                                                         
                                                         #Infobulle
                                                         dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle5']['label']][language], 
                                                                     target=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['id'], 
                                                                     id=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle5']['id'], 
                                                                     className=app_content['div_box']['box study area']['choice']['radius']['button infobulle5']['infobulle5']['class']),
                                                         
                                                         #Menu déroulant pour les communes
                                                         dcc.Dropdown(id=app_content['div_box']['box study area']['choice']['radius']['municipality input area']['id'], 
                                                                      className='zone_input',
                                                                      multi=False), 
                                                         
                                                         #Texte pour le l'input
                                                         html.P(app_translation[app_content['div_box']['box study area']['choice']['radius']['radius research txt']['label']][language]),
                                                        
                                                        #Input valeur rayon
                                                         dcc.Input(id=app_content['div_box']['box study area']['choice']['radius']['radius input area']['id'], 
                                                                   className='zone_input'),
                                                         
                                                     ]
                                     ),
                                     
                                     #Onlgets calcul par communes
                                     dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['municipality']['title']['label']][language],
                                             value='tab-municipality',
                                             className=app_content['div_box']['box study area']['choice']['class'],
                                             children=[#texte input
                                                       html.P(app_translation[app_content['div_box']['box study area']['choice']['municipality']['municipality origin txt']['label']][language]),
                                                       
                                                       #Bouton pour l'infobulle
                                                       html.Button("?", 
                                                                   className=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['class'], 
                                                                   id=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['id'], 
                                                                   disabled=True),
                                                       
                                                       #Infobulle
                                                       dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['label']][language],
                                                                   target=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['id'], 
                                                                   id=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['id'], 
                                                                   className=app_content['div_box']['box study area']['choice']['municipality']['button infobulle6']['infobulle6']['class']),
                                                       
                                                       #Menu déroulant commune d'origine
                                                       dcc.Dropdown(id=app_content['div_box']['box study area']['choice']['municipality']['municipality input area']['id'],
                                                                    className='zone_input',
                                                                    multi=False),
                                                       
                                                       #Texte input
                                                       html.P(app_translation[app_content['div_box']['box study area']['choice']['municipality']['list municipality']['label']][language]),
                                                       
                                                       #Menu déroulant liste des communes
                                                       dcc.Dropdown(id=app_content['div_box']['box study area']['choice']['municipality']['list municipality']['id'],
                                                                    className='zone_input',
                                                                    multi=True),     
                                                     ]
                                     ),
                                     #Onlget calcul par département
                                     dcc.Tab(label=app_translation[app_content['div_box']['box study area']['choice']['county']['title']['label']][language], 
                                             value='tab-county',
                                             className=app_content['div_box']['box study area']['choice']['class'],
                                             children=[#Texte input
                                                       html.P("Liste de départements"),
                                                       
                                                       #Bouton pour l'infobulle
                                                       html.Button("?", 
                                                                   className=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['class'],
                                                                   id=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['id'],
                                                                   disabled=True),
                                                       
                                                       #Infobulle
                                                       dbc.Tooltip(children=app_translation[app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle7']['label']][language],
                                                                   target=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['id'], 
                                                                   id=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle7']['id'], 
                                                                   className=app_content['div_box']['box study area']['choice']['county']['button infobulle7']['infobulle7']['class']),
                                                       
                                                       #Liste déroulante départements
                                                       dcc.Dropdown(id=app_content['div_box']['box study area']['choice']['county']['list county']['id'],
                                                                    className='zone_input',
                                                                    multi=True),
                                                     ]
                                     )
                     ])
                 ], className=app_content['div_box']['class']),

             html.Div( #Div contenant le choix des catégories S-P
                 [   #Titre de la boîte
                     html.H2(children=app_translation[app_content['div_box']['box csp']['title']['label']][language], 
                             className=app_content['div_box']['box csp']['title']['class'],
                             id=app_content['div_box']['box csp']['title']['id']),
                     
                     #Bouton pour l'infobulle
                     html.Button("?", className=app_content['div_box']['box csp']['button infobulle8']['class'],
                                 id=app_content['div_box']['box csp']['button infobulle8']['id'],
                                 disabled=True),
                     
                     #Infobulle
                     dbc.Tooltip(children=app_translation[app_content['div_box']['box csp']['button infobulle8']['infobulle8']['label']][language], 
                                 target=app_content['div_box']['box csp']['button infobulle8']['id'], 
                                 id=app_content['div_box']['box csp']['button infobulle8']['infobulle8']['id'], 
                                 className=app_content['div_box']['box csp']['button infobulle8']['infobulle8']['class']),
                     
                     #Liste de cases à cocher pour les CSP
                     dcc.Checklist(options=[app_translation[o][language] for o in app_content['div_box']['box csp']['choice csp']['options']], 
                                   value=[app_translation[o][language] for o in app_content['div_box']['box csp']['choice csp']['options']],
                                   id=app_content['div_box']['box csp']['choice csp']['id'])
                 ], className=app_content['div_box']['class']),
             
             #Bouton lancer la simulation
             html.Button(children=app_translation[app_content['button launch simulation']['label']][language], 
                         id=app_content['button launch simulation']['id']),
             
             
             html.Div( #Div contenant les bouton de téléchargement
                 children=[
                     #Bouton téléchargement CSV
                     html.Button(children=app_translation[app_content['button dl csv']['label']][language], 
                                 id=app_content['button dl csv']['id'],
                                 className=app_content['button dl csv']['class']),
                     
                     #Bouton téléchargement SVG
                     html.Button(children=app_translation[app_content['button dl svg']['label']][language], 
                                 id=app_content['button dl svg']['id'],
                                 className=app_content['button dl svg']['class'])
                 ], id="dl_container"),
             
             #Bouton paramètres
             html.Button(children=app_translation[app_content['button settings']['label']][language],
                         id=app_content['button settings']['id']),
             
             #Liste déroulante langues
             dcc.Dropdown(options=[{'label': app_content['dropdown language']['languages'][key],
                                     'value': key} for key in app_content['dropdown language']['languages'].keys()],
                          value=language,
                          clearable=False,
                          id=app_content['dropdown language']['id']),
            
            #Affichage temporaire des résultats
             html.Div(id="text")
        ]
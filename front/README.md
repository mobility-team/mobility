# Front — Interface Mobility

Ce dossier contient l’interface Dash/Mantine de l’application Mobility.

## Lancer l’interface

Depuis la racine du projet, exécuter :

cd front
python -m app.pages.main.main


L’application démarre alors en mode développement sur http://127.0.0.1:8050/.

## Structure simplifiée

app/components/ — Composants UI (carte, panneaux, contrôles, etc.)

app/services/ — Services pour la génération des scénarios et l’accès aux données

app/pages/main/ — Page principale et point d’entrée (main.py)

app/callbacks.py — Callbacks Dash
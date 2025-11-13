from unittest.mock import patch

# Mock input avant d'importer main, pour éviter l'appel interactif
with patch("builtins.input", return_value="Yes"):
    from front.app.pages.main import main


def test_import_main_and_create_app():
    app = main.create_app()
    assert app is not None

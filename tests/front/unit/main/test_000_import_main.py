from front.app.pages.main import main

def test_import_main_and_create_app():
    app = main.create_app()
    assert app is not None

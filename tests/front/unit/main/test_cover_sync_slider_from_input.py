import importlib
import dash
from dash import no_update
from unittest.mock import patch

# Mock input avant d'importer main pour éviter le prompt interactif
with patch("builtins.input", return_value="Yes"):
    import front.app.pages.main.main as main


def test_cover_sync_slider_from_input(monkeypatch):
    captured = []

    real_callback = dash.Dash.callback

    def recording_callback(self, *outputs, **kwargs):
        def decorator(func):
            captured.append((outputs, kwargs, func))
            return func
        return decorator

    monkeypatch.setattr(dash.Dash, "callback", recording_callback, raising=True)
    importlib.reload(main)
    app = main.create_app()
    monkeypatch.setattr(dash.Dash, "callback", real_callback, raising=True)

    target = None
    for _outs, _kw, func in captured:
        if getattr(func, "__name__", "") == "_sync_slider_from_input":
            target = func
            break

    assert target is not None, "Callback _sync_slider_from_input introuvable"
    assert target(None, 10) is no_update
    assert target(10, 10) is no_update
    assert target(8, 10) == 8

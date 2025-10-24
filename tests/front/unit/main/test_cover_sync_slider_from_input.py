import importlib
import dash
from dash import no_update
import front.app.pages.main.main as main


def test_cover_sync_slider_from_input(monkeypatch):
    captured = []  # (outputs_tuple, kwargs, func)

    real_callback = dash.Dash.callback

    def recording_callback(self, *outputs, **kwargs):
        def decorator(func):
            captured.append((outputs, kwargs, func))
            return func  # important: laisser Dash enregistrer la même fonction
        return decorator

    # 1) Capturer l’enregistrement des callbacks pendant create_app()
    monkeypatch.setattr(dash.Dash, "callback", recording_callback, raising=True)
    importlib.reload(main)
    app = main.create_app()
    monkeypatch.setattr(dash.Dash, "callback", real_callback, raising=True)

    # 2) Retrouver directement la fonction par son nom
    target = None
    for _outs, _kw, func in captured:
        if getattr(func, "__name__", "") == "_sync_slider_from_input":
            target = func
            break

    assert target is not None, "Callback _sync_slider_from_input introuvable"

    assert target(None, 10) is no_update      # branche input_val is None
    assert target(10, 10) is no_update        # branche input_val == current_slider
    assert target(8, 10) == 8                 # branche return input_val

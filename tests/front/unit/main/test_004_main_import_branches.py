import importlib
import builtins
import types
import sys

def test_main_uses_service_branch(monkeypatch):
    # S'assure que le module service existe
    mod = types.ModuleType("front.app.services.map_service")
    def fake_get_map_deck_json_from_scn(scn, opts=None):
        return "__deck_from_service__"
    mod.get_map_deck_json_from_scn = fake_get_map_deck_json_from_scn

    # Enregistre la hiérarchie dans sys.modules (si besoin)
    sys.modules.setdefault("front", types.ModuleType("front"))
    sys.modules.setdefault("front.app", types.ModuleType("front.app"))
    sys.modules.setdefault("front.app.services", types.ModuleType("front.app.services"))
    sys.modules["front.app.services.map_service"] = mod

    from front.app.pages.main import main
    importlib.reload(main)

    assert main.USE_MAP_SERVICE is True
    out = main._make_deck_json_from_scn({"k": "v"})
    assert out == "__deck_from_service__"


def test_main_uses_fallback_branch(monkeypatch):
    # Force l'import de map_service à échouer pendant le reload
    import front.app.pages.main.main as main
    importlib.reload(main)  # recharge une base propre

    real_import = builtins.__import__
    def fake_import(name, *a, **kw):
        if name == "front.app.services.map_service":
            raise ImportError("Simulated ImportError for test")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    importlib.reload(main)

    assert main.USE_MAP_SERVICE is False

    # On monkeypatch la fabrique fallback pour éviter de dépendre du geo
    called = {}
    def fake_make_deck_json(scn, opts):
        called["ok"] = True
        return "__deck_from_fallback__"

    monkeypatch.setattr(
        "front.app.pages.main.main.make_deck_json",
        fake_make_deck_json,
        raising=True,
    )
    out = main._make_deck_json_from_scn({"k": "v"})
    assert out == "__deck_from_fallback__"
    assert called.get("ok") is True

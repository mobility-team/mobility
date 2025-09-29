from importlib import import_module, reload
from pathlib import Path
import sys
import types
import pytest
import pandas as pd

@pytest.fixture(scope="session", autouse=True)
def _ensure_real_mobility_asset_module():
    """
    Make sure mobility.asset is imported from your source tree and not replaced by any
    higher-level test double. We reload the module to restore the real class.
    """
    # If the module was never imported, import it; if it was imported (possibly stubbed), reload it.
    try:
        mod = sys.modules.get("mobility.asset")
        if mod is None:
            import_module("mobility.asset")
        else:
            reload(mod)
    except Exception as exc:  # surface import problems early and clearly
        pytest.skip(f"Cannot import mobility.asset: {exc}")


# ------------------------------------------------------------
# No-op rich.progress.Progress (safe even if rich is not present)
# ------------------------------------------------------------
@pytest.fixture(autouse=True)
def no_op_progress(monkeypatch):
    class _NoOpProgress:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def add_task(self, *a, **k): return 0
        def update(self, *a, **k): return None
        def advance(self, *a, **k): return None
        def track(self, iterable, *a, **k):
            for x in iterable:
                yield x
        def stop(self): return None

    try:
        import rich.progress as rp
        monkeypatch.setattr(rp, "Progress", _NoOpProgress, raising=True)
    except Exception:
        pass


# ----------------------------------------------------------------------
# Patch NumPy private _methods to ignore the _NoValue sentinel (pandas interop)
# ----------------------------------------------------------------------
@pytest.fixture(autouse=True)
def patch_numpy__methods(monkeypatch):
    try:
        from numpy.core import _methods as _np_methods
        from numpy import _NoValue as _NP_NoValue
    except Exception:
        return

    def _wrap(func):
        def _wrapped(a, axis=None, dtype=None, out=None,
                    keepdims=_NP_NoValue, initial=_NP_NoValue, where=_NP_NoValue):
            if keepdims is _NP_NoValue:
                keepdims = False
            if initial is _NP_NoValue:
                initial = None
            if where is _NP_NoValue:
                where = True
            return func(a, axis=axis, dtype=dtype, out=out,
                        keepdims=keepdims, initial=initial, where=where)
        return _wrapped

    if hasattr(_np_methods, "_sum"):
        monkeypatch.setattr(_np_methods, "_sum", _wrap(_np_methods._sum), raising=True)
    if hasattr(_np_methods, "_amax"):
        monkeypatch.setattr(_np_methods, "_amax", _wrap(_np_methods._amax), raising=True)


# ---------------------------------------------------------
# Parquet stubs helper (for future cache read/write tests)
# ---------------------------------------------------------
@pytest.fixture
def parquet_stubs(monkeypatch):
    state = {
        "last_written_path": None,
        "last_read_path": None,
        "reads": 0,
        "writes": 0,
        "read_return_df": pd.DataFrame({"__empty__": []}),
    }

    def _read(path, *a, **k):
        state["last_read_path"] = Path(path)
        state["reads"] += 1
        return state["read_return_df"]

    def _write(self, path, *a, **k):
        state["last_written_path"] = Path(path)
        state["writes"] += 1

    class Controller:
        @property
        def last_written_path(self): return state["last_written_path"]
        @property
        def last_read_path(self): return state["last_read_path"]
        @property
        def reads(self): return state["reads"]
        @property
        def writes(self): return state["writes"]
        def stub_read(self, df):
            state["read_return_df"] = df
            monkeypatch.setattr(pd, "read_parquet", _read, raising=True)
        def capture_writes(self):
            monkeypatch.setattr(pd.DataFrame, "to_parquet", _write, raising=True)

    return Controller()


# ---------------------------------------------------------
# Provide the canonical base class for abstract method coverage
# ---------------------------------------------------------
@pytest.fixture
def asset_base_class(_ensure_real_mobility_asset_module):
    from mobility.asset import Asset
    # Sanity: ensure this is the real class (has the abstract 'get' attribute)
    assert hasattr(Asset, "get"), "mobility.asset.Asset does not define .get; a stub may be shadowing it"
    return Asset


# ---------------------------------------------------------
# Keep compatibility with tests that still request this fixture
# ---------------------------------------------------------
@pytest.fixture
def use_real_asset_init(asset_base_class):
    """
    Back-compat fixture: returns the real Asset class (we are not stubbing __init__).
    Tests that request this can continue to do so without changes.
    """
    return asset_base_class

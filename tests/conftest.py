# tests/conftest.py
import pytest
import importlib
import numpy as np

@pytest.fixture(scope="session", autouse=True)
def guard_numpy_reload():
    """Avoid importlib.reload(numpy) during the session — it can corrupt pandas/np internals."""
    orig_reload = importlib.reload

    def guarded_reload(mod):
        if getattr(mod, "__name__", "") == "numpy":
            raise RuntimeError(
                "NumPy reload detected. Do not reload numpy in tests; "
                "it can cause _NoValueType errors in pandas."
            )
        return orig_reload(mod)

    importlib.reload = guarded_reload
    try:
        yield
    finally:
        importlib.reload = orig_reload


@pytest.fixture(scope="session", autouse=True)
def patch_numpy__methods():
    """
    Shim numpy.core._methods._amax and _sum so that passing initial=np._NoValue
    doesn't reach the ufunc layer (which raises TypeError in some environments).
    """
    try:
        from numpy.core import _methods as _nm  # private, but stable enough for tests
    except Exception:
        # If layout differs in your NumPy build, just no-op.
        yield
        return

    # Keep originals
    orig_amax = getattr(_nm, "_amax", None)
    orig_sum  = getattr(_nm, "_sum", None)

    # If missing for some reason, just no-op
    if orig_amax is None or orig_sum is None:
        yield
        return

    def safe_amax(a, axis=None, out=None, keepdims=False, initial=np._NoValue, where=True):
        # If initial is the sentinel, avoid sending it to the ufunc
        if initial is np._NoValue:
            return np.max(a, axis=axis, out=out, keepdims=keepdims, where=where)
        return orig_amax(a, axis=axis, out=out, keepdims=keepdims, initial=initial, where=where)

    def safe_sum(a, axis=None, dtype=None, out=None, keepdims=False, initial=np._NoValue, where=True):
        if initial is np._NoValue:
            return np.sum(a, axis=axis, dtype=dtype, out=out, keepdims=keepdims, where=where)
        return orig_sum(a, axis=axis, dtype=dtype, out=out, keepdims=keepdims, initial=initial, where=where)

    # Patch
    _nm._amax = safe_amax
    _nm._sum  = safe_sum

    try:
        yield
    finally:
        # Restore
        _nm._amax = orig_amax
        _nm._sum  = orig_sum


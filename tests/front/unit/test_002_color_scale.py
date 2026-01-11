import pandas as pd
import numpy as np
from pytest import approx

from app.components.features.map.color_scale import fit_color_scale


def test_fit_color_scale_basic():
    s = pd.Series([10.0, 20.0, 25.0, 30.0])
    scale = fit_color_scale(s)

    # vmin/vmax basés sur les percentiles 5/95 (et pas min/max stricts)
    vmin_expected = float(np.nanpercentile(s, 5))
    vmax_expected = float(np.nanpercentile(s, 95))
    assert scale.vmin == approx(vmin_expected)
    assert scale.vmax == approx(vmax_expected)

    # Couleur à vmin ~ froide, à vmax ~ chaude
    c_min = scale.rgba(s.min())
    c_mid = scale.rgba(s.mean())
    c_max = scale.rgba(s.max())
    assert isinstance(c_min, list) and len(c_min) == 4
    assert c_min[0] < c_max[0]  # rouge augmente
    assert c_min[2] > c_max[2]  # bleu diminue

    # Légende numérique "x.x min"
    lg = scale.legend(11.0)
    assert isinstance(lg, str)
    assert lg.endswith(" min")

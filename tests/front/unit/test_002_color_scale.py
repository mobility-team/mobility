import pandas as pd
from app.components.features.map.color_scale import fit_color_scale

def test_fit_color_scale_basic():
    s = pd.Series([10.0, 20.0, 25.0, 30.0])
    scale = fit_color_scale(s)
    assert scale.vmin == 10.0
    assert scale.vmax == 30.0

    # Couleur à vmin ~ froide, à vmax ~ chaude
    c_min = scale.rgba(10.0)
    c_mid = scale.rgba(20.0)
    c_max = scale.rgba(30.0)
    assert isinstance(c_min, list) and len(c_min) == 4
    assert c_min[0] < c_max[0]  # rouge augmente
    assert c_min[2] > c_max[2]  # bleu diminue

    # Légende cohérente
    assert scale.legend(11.0) in {"Accès rapide", "Accès moyen", "Accès lent"}

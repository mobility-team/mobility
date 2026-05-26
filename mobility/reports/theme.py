"""Shared Plotly style for Mobility report figures."""

from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio

MOBILITY_COLORS = {
    "inner_zone": "#3C4658",
    "outer_zone": "#DCE1E8",
    "survey": "#3C4658",
    "model": "#E41A1C",
    "zone_border": "#FFFFFF",
    "model_border": "#6F7378",
    "inner_border": "#7A7F86",
    "country_border": "#111111",
    "label": "#2B2B2B",
    "background": "#FFFFFF",
    "grid": "#D9D9D9",
}

MOBILITY_TEMPLATE = "mobility_report"


def register_mobility_template() -> None:
    """Register the shared Mobility Plotly template if needed."""
    if MOBILITY_TEMPLATE in pio.templates:
        return

    pio.templates[MOBILITY_TEMPLATE] = go.layout.Template(
        layout=go.Layout(
            font={"family": "Arial, sans-serif", "color": MOBILITY_COLORS["label"]},
            paper_bgcolor=MOBILITY_COLORS["background"],
            plot_bgcolor=MOBILITY_COLORS["background"],
            margin={"l": 20, "r": 20, "t": 55, "b": 20},
            legend={
                "orientation": "h",
                "yanchor": "bottom",
                "y": 0.01,
                "xanchor": "left",
                "x": 0.01,
                "bgcolor": "rgba(255,255,255,0.82)",
            },
            xaxis={"gridcolor": MOBILITY_COLORS["grid"], "zeroline": False},
            yaxis={"gridcolor": MOBILITY_COLORS["grid"], "zeroline": False},
        )
    )


def apply_report_layout(fig: go.Figure, title: str | None = None) -> go.Figure:
    """Apply the shared Mobility report layout to a Plotly figure."""
    register_mobility_template()
    fig.update_layout(template=MOBILITY_TEMPLATE)
    if title is not None:
        fig.update_layout(title={"text": title, "x": 0.02, "xanchor": "left"})
    return fig

from __future__ import annotations

import math
from typing import Dict, Iterable, Optional

import altair as alt
import pandas as pd

try:
    import networkx as nx
except Exception:  # pragma: no cover
    nx = None

try:
    import plotly.graph_objects as go
except Exception:  # pragma: no cover
    go = None


def network_deps_available() -> bool:
    return (nx is not None) and (go is not None)


def network_deps_hint() -> str:
    if network_deps_available():
        return ""
    return (
        "Network graph is unavailable because `networkx` and/or `plotly` is not installed in the current Python environment. "
        "If you have them in your project venv, run Streamlit via that venv: "
        "`<venv>/Scripts/python.exe -m streamlit run app.py`."
    )


def heatmap_rect(
    df_long: pd.DataFrame,
    x: str,
    y: str,
    value: str,
    title: str,
    height: int = 420,
):
    if df_long is None or df_long.empty:
        return None

    chart = (
        alt.Chart(df_long)
        .mark_rect()
        .encode(
            x=alt.X(f"{x}:N", title=None),
            y=alt.Y(f"{y}:N", title=None, sort='-x'),
            color=alt.Color(f"{value}:Q", title="Shared playlists"),
            tooltip=[alt.Tooltip(f"{x}:N"), alt.Tooltip(f"{y}:N"), alt.Tooltip(f"{value}:Q")],
        )
        .properties(title=title, height=height)
    )
    return chart


def network_figure(
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    node_id_col: str = "id",
    node_label_col: str = "label",
    node_group_col: str = "group",
    src_col: str = "src",
    dst_col: str = "dst",
    weight_col: str = "weight",
    title: str = "Relationship network",
    max_edges: int = 120,
):
    if nx is None or go is None:
        # Optional dependency: allow the rest of the explanation page (heatmaps/tables)
        # to work even if the environment doesn't have graph libs installed.
        return None

    if nodes is None or nodes.empty or edges is None or edges.empty:
        return None

    edges = edges.copy()
    edges[weight_col] = pd.to_numeric(edges[weight_col], errors="coerce").fillna(0)
    edges = edges.sort_values(weight_col, ascending=False).head(int(max_edges))

    g = nx.Graph()

    for _, r in nodes.iterrows():
        g.add_node(str(r[node_id_col]), label=str(r.get(node_label_col, r[node_id_col])), group=str(r.get(node_group_col, "")))

    for _, r in edges.iterrows():
        s = str(r[src_col])
        t = str(r[dst_col])
        w = float(r.get(weight_col, 0))
        if w <= 0:
            continue
        if s not in g.nodes or t not in g.nodes:
            continue
        g.add_edge(s, t, weight=w)

    if g.number_of_edges() == 0:
        return None

    pos = nx.spring_layout(g, seed=7, k=0.85 / math.sqrt(max(g.number_of_nodes(), 1)))

    # Edge traces
    edge_x = []
    edge_y = []
    edge_text = []
    edge_width = []

    for u, v, data in g.edges(data=True):
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]
        w = float(data.get("weight", 1.0))
        edge_text.append(f"{g.nodes[u]['label']} ↔ {g.nodes[v]['label']}\nShared playlists: {int(w)}")
        edge_width.append(w)

    # Normalize width a bit
    w_max = max(edge_width) if edge_width else 1.0
    width_scaled = [1 + 5 * (w / w_max) for w in edge_width]

    # Plotly needs a single width, so approximate by duplicating edges per segment not worth it.
    # Keep constant line width + show weight in hover; readability stays fine.
    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=1, color="#888"),
        hoverinfo="text",
        text=edge_text,
        mode="lines",
        name="",
    )

    # Node traces (split by group for color)
    groups = list(dict.fromkeys([str(g.nodes[n].get("group", "")) for n in g.nodes]))
    palette = ["#1DB954", "#3B82F6", "#F59E0B", "#EF4444", "#A855F7", "#14B8A6"]
    group_to_color = {grp: palette[i % len(palette)] for i, grp in enumerate(groups)}

    node_traces = []
    for grp in groups:
        xs = []
        ys = []
        texts = []
        labels = []
        for n in g.nodes:
            if str(g.nodes[n].get("group", "")) != grp:
                continue
            x, y = pos[n]
            xs.append(x)
            ys.append(y)
            label = str(g.nodes[n].get("label", n))
            labels.append(label)
            texts.append(label)

        node_traces.append(
            go.Scatter(
                x=xs,
                y=ys,
                mode="markers+text",
                text=[l if len(l) <= 26 else (l[:23] + "…") for l in labels],
                textposition="top center",
                hoverinfo="text",
                hovertext=texts,
                marker=dict(size=12, color=group_to_color.get(grp, "#999"), line=dict(width=1, color="#111")),
                name=grp or "Nodes",
            )
        )

    fig = go.Figure(data=[edge_trace] + node_traces)
    fig.update_layout(
        title=title,
        showlegend=True,
        margin=dict(l=10, r=10, t=40, b=10),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=560,
    )
    return fig

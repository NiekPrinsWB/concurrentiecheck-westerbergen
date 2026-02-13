"""Plotly chart helpers met Westerbergen brand-styling."""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from .styles import COLORS, CHART_COLORS, DATA_COLORS


# ── Base Layout ─────────────────────────────────────────────────
BASE_LAYOUT = dict(
    template="plotly_white",
    font=dict(family="Inter, Helvetica, sans-serif", color=COLORS["tekst_donker"], size=12),
    title_font=dict(family="Playfair Display, Georgia, serif", color=COLORS["diep_bosgroen"], size=16),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=20, r=20, t=50, b=20),
    hoverlabel=dict(
        bgcolor=COLORS["diep_bosgroen"],
        font_color=COLORS["wit"],
        font_size=13,
        font_family="Inter, Helvetica, sans-serif",
    ),
    legend=dict(
        font=dict(size=11),
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor=COLORS["natuurlijk_beige"],
        borderwidth=1,
    ),
)


def _apply_base(fig: go.Figure, title: str = None, height: int = 400) -> go.Figure:
    """Apply base layout + optional title."""
    layout = dict(BASE_LAYOUT)
    if title:
        layout["title_text"] = title
    layout["height"] = height
    fig.update_layout(**layout)
    fig.update_xaxes(gridcolor=COLORS["natuurlijk_beige"], gridwidth=0.5)
    fig.update_yaxes(gridcolor=COLORS["natuurlijk_beige"], gridwidth=0.5)
    return fig


# ── Chart: Prijsindex Horizontal Bar ────────────────────────────
def chart_price_index_bars(price_index: list[dict]) -> go.Figure:
    """Horizontale bar chart: gemiddelde prijsindex per concurrent."""
    df = pd.DataFrame(price_index)
    if df.empty:
        return go.Figure()

    avg = df.groupby("competitor")["price_index"].mean().sort_values(ascending=True)

    colors = []
    for val in avg.values:
        if val < 85:
            colors.append(COLORS["diep_bosgroen"])
        elif val < 115:
            colors.append(COLORS["zandgroen"])
        else:
            colors.append(COLORS["oranje_bruin"])

    fig = go.Figure(go.Bar(
        x=avg.values,
        y=avg.index,
        orientation="h",
        marker_color=colors,
        text=[f"{v:.0f}" for v in avg.values],
        textposition="auto",
        textfont=dict(color=COLORS["wit"], size=13, family="Inter"),
    ))

    fig.add_vline(x=100, line_dash="dot", line_color=COLORS["natuurlijk_beige"], line_width=2)
    fig.add_annotation(x=100, y=-0.15, yref="paper", text="100 = pariteit",
                       showarrow=False, font=dict(size=10, color=COLORS["tekst_licht"]))

    return _apply_base(fig, "Gemiddelde prijsindex per concurrent", height=max(250, len(avg) * 55))


# ── Chart: Ranking Donut ────────────────────────────────────────
def chart_ranking_donut(competitive_position: list[dict]) -> go.Figure:
    """Donut chart: % van gevallen #1, #2, #3+."""
    if not competitive_position:
        return go.Figure()

    rank1 = sum(1 for p in competitive_position if p["wb_rank"] == 1)
    rank2 = sum(1 for p in competitive_position if p["wb_rank"] == 2)
    rank3 = sum(1 for p in competitive_position if p["wb_rank"] >= 3)
    total = rank1 + rank2 + rank3

    pct1 = rank1 / total * 100 if total > 0 else 0

    fig = go.Figure(go.Pie(
        labels=["#1 Goedkoopst", "#2", "#3 of lager"],
        values=[rank1, rank2, rank3],
        hole=0.65,
        marker_colors=[COLORS["diep_bosgroen"], COLORS["zandgroen"], COLORS["oranje_bruin"]],
        textinfo="percent",
        textfont=dict(size=13, family="Inter", color=COLORS["wit"]),
        hovertemplate="%{label}: %{value}x (%{percent})<extra></extra>",
    ))

    fig.add_annotation(
        text=f"<b>{pct1:.0f}%</b><br><span style='font-size:11px;color:{COLORS['tekst_licht']}'>Goedkoopst</span>",
        x=0.5, y=0.5, xref="paper", yref="paper",
        showarrow=False,
        font=dict(size=24, family="Playfair Display, serif", color=COLORS["diep_bosgroen"]),
    )

    return _apply_base(fig, "Concurrentiepositie", height=300)


# ── Chart: Seizoenslijn ─────────────────────────────────────────
def chart_seasonal_lines(seasonal_patterns: dict, competitors: list[str]) -> go.Figure:
    """Line chart: gemiddelde prijs per maand, WB als dikke lijn."""
    by_month = seasonal_patterns.get("by_month", {})
    if not by_month:
        return go.Figure()

    fig = go.Figure()
    months = sorted(by_month.keys())

    # Concurrenten als dunne lijnen
    for i, comp in enumerate(competitors):
        prices = []
        for m in months:
            data = by_month[m].get(comp, {})
            prices.append(data.get("avg_price"))

        color = CHART_COLORS[(i + 1) % len(CHART_COLORS)]
        fig.add_trace(go.Scatter(
            x=months, y=prices, name=comp, mode="lines+markers",
            line=dict(color=color, width=1.5, dash="dot"),
            marker=dict(size=5),
            hovertemplate=f"{comp}<br>%{{x}}: €%{{y:,.0f}}<extra></extra>",
        ))

    # Westerbergen als dikke lijn (bovenop)
    wb_prices = [by_month[m].get("Westerbergen", {}).get("avg_price") for m in months]
    fig.add_trace(go.Scatter(
        x=months, y=wb_prices, name="Westerbergen", mode="lines+markers",
        line=dict(color=COLORS["diep_bosgroen"], width=3.5),
        marker=dict(size=8, symbol="diamond"),
        hovertemplate="Westerbergen<br>%{x}: €%{y:,.0f}<extra></extra>",
    ))

    return _apply_base(fig, "Seizoenspatroon: gemiddelde prijs per maand", height=380)


# ── Chart: Revenue Waterfall ────────────────────────────────────
def chart_revenue_waterfall(recommendations: list[dict]) -> go.Figure:
    """Waterfall chart: potentiële extra omzet per maand."""
    if not recommendations:
        return go.Figure()

    df = pd.DataFrame(recommendations)
    df["month"] = pd.to_datetime(df["check_in_date"]).dt.strftime("%Y-%m")
    monthly = df.groupby("month")["extra_omzet"].sum().sort_index()

    fig = go.Figure(go.Waterfall(
        x=monthly.index,
        y=monthly.values,
        text=[f"€{v:,.0f}" for v in monthly.values],
        textposition="outside",
        textfont=dict(size=11, family="Inter"),
        connector=dict(line=dict(color=COLORS["natuurlijk_beige"])),
        increasing=dict(marker_color=COLORS["diep_bosgroen"]),
        totals=dict(marker_color=COLORS["zandgroen"]),
    ))

    return _apply_base(fig, "Potentiële extra omzet per maand", height=350)


# ── Chart: Prijs per Nacht Grouped Bars ─────────────────────────
def chart_ppn_bars(price_per_night: list[dict], competitors: list[str]) -> go.Figure:
    """Grouped bar chart: gem. prijs per nacht per verblijfstype."""
    if not price_per_night:
        return go.Figure()

    df = pd.DataFrame(price_per_night)

    # WB gemiddelde per stay_type
    wb_avg = df.groupby("stay_type")["wb_ppn"].mean()

    # Competitor gemiddelden
    comp_avgs = {}
    for comp in competitors:
        vals = []
        for _, row in df.iterrows():
            comp_data = row.get("competitors", {})
            if comp in comp_data and comp_data[comp] is not None:
                vals.append(comp_data[comp])
        if vals:
            # Bereken per stay_type
            comp_by_type = {}
            for _, row in df.iterrows():
                st = row["stay_type"]
                comp_data = row.get("competitors", {})
                if comp in comp_data and comp_data[comp] is not None:
                    comp_by_type.setdefault(st, []).append(comp_data[comp])
            comp_avgs[comp] = {st: sum(v)/len(v) for st, v in comp_by_type.items()}

    stay_types = sorted(wb_avg.index)
    fig = go.Figure()

    # WB bars
    fig.add_trace(go.Bar(
        x=stay_types, y=[wb_avg.get(st, 0) for st in stay_types],
        name="Westerbergen",
        marker_color=COLORS["diep_bosgroen"],
        text=[f"€{wb_avg.get(st, 0):,.0f}" for st in stay_types],
        textposition="auto",
        textfont=dict(color=COLORS["wit"], size=11),
    ))

    # Competitor bars
    for i, comp in enumerate(competitors):
        if comp in comp_avgs:
            fig.add_trace(go.Bar(
                x=stay_types,
                y=[comp_avgs[comp].get(st, 0) for st in stay_types],
                name=comp,
                marker_color=CHART_COLORS[(i + 1) % len(CHART_COLORS)],
            ))

    fig.update_layout(barmode="group")
    return _apply_base(fig, "Gemiddelde prijs per nacht per verblijfstype", height=400)


# ── Chart: Scatter WB vs Concurrent ─────────────────────────────
def chart_scatter_comparison(price_index: list[dict], competitor: str) -> go.Figure:
    """Scatter: WB prijs vs concurrent prijs per datum."""
    df = pd.DataFrame(price_index)
    df = df[df["competitor"] == competitor].copy()
    if df.empty:
        return go.Figure()

    # Kleur op basis van index
    df["color"] = df["price_index"].apply(
        lambda x: COLORS["diep_bosgroen"] if x < 85
        else COLORS["zandgroen"] if x < 115
        else COLORS["oranje_bruin"]
    )

    fig = go.Figure()

    # Diagonaal (pariteit)
    max_price = max(df["wb_price"].max(), df["comp_price"].max()) * 1.1
    fig.add_trace(go.Scatter(
        x=[0, max_price], y=[0, max_price],
        mode="lines", line=dict(color=COLORS["natuurlijk_beige"], dash="dash", width=1.5),
        showlegend=False, hoverinfo="skip",
    ))

    fig.add_trace(go.Scatter(
        x=df["comp_price"], y=df["wb_price"],
        mode="markers",
        marker=dict(color=df["color"], size=9, line=dict(width=0.5, color=COLORS["wit"])),
        text=df.apply(lambda r: f"{r['check_in_date']}, {r['nights']}n", axis=1),
        hovertemplate="%{text}<br>WB: €%{y:,.0f}<br>" + competitor + ": €%{x:,.0f}<extra></extra>",
        showlegend=False,
    ))

    fig.update_xaxes(title_text=f"Prijs {competitor} (€)")
    fig.update_yaxes(title_text="Prijs Westerbergen (€)")

    fig.add_annotation(
        text="Boven lijn = WB duurder", x=0.95, y=0.05,
        xref="paper", yref="paper", showarrow=False,
        font=dict(size=10, color=COLORS["tekst_licht"]),
    )

    return _apply_base(fig, f"Westerbergen vs {competitor}", height=400)


# ── Chart: Heatmap Prijsindex ───────────────────────────────────
def chart_index_heatmap(price_index: list[dict], competitors: list[str]) -> go.Figure:
    """Heatmap: prijsindex per week × concurrent."""
    df = pd.DataFrame(price_index)
    if df.empty:
        return go.Figure()

    df["week"] = pd.to_datetime(df["check_in_date"]).dt.isocalendar().week
    df["week_label"] = "W" + df["week"].astype(str)

    pivot = df.pivot_table(values="price_index", index="competitor", columns="week_label", aggfunc="mean")
    pivot = pivot.reindex([c for c in competitors if c in pivot.index])

    # Sorteer kolommen op weeknummer
    cols = sorted(pivot.columns, key=lambda x: int(x[1:]))
    pivot = pivot[cols]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale=[
            [0, COLORS["diep_bosgroen"]],
            [0.5, COLORS["zandgroen"]],
            [1, COLORS["oranje_bruin"]],
        ],
        zmin=40, zmax=140,
        text=[[f"{v:.0f}" if pd.notna(v) else "" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=10),
        hovertemplate="Concurrent: %{y}<br>Week: %{x}<br>Index: %{z:.0f}<extra></extra>",
        colorbar=dict(title=dict(text="Index", side="right")),
    ))

    return _apply_base(fig, "Prijsindex per week (< 100 = WB goedkoper)", height=max(250, len(pivot) * 45 + 100))

"""Westerbergen Pricing Intelligence — Marktpositie Dashboard.

Geeft op elk detailniveau duidelijkheid over waar Westerbergen
qua prijs in de markt zit ten opzichte van 6 concurrenten.

Start: python -m streamlit run streamlit_app/app.py
"""

import os
import sys
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# ── Pad setup ───────────────────────────────────────────────────
_app_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_app_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from components.styles import get_custom_css, COLORS, CHART_COLORS, STAY_LABELS
from components.data_loader import get_db_path, load_analytics, get_available_dates, get_scrape_status

# ── Page Config ─────────────────────────────────────────────────
st.set_page_config(
    page_title="Westerbergen — Marktpositie",
    page_icon="🌲",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(get_custom_css(), unsafe_allow_html=True)

# ── Plotly defaults ─────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    template="plotly_white",
    font=dict(family="Inter, Helvetica, sans-serif", color=COLORS["tekst_donker"], size=12),
    title_font=dict(family="Playfair Display, Georgia, serif", color=COLORS["diep_bosgroen"], size=16),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=20, r=20, t=50, b=20),
    hoverlabel=dict(bgcolor=COLORS["diep_bosgroen"], font_color="#fff", font_size=13),
    legend=dict(font=dict(size=11), bgcolor="rgba(255,255,255,0.8)",
                bordercolor=COLORS["natuurlijk_beige"], borderwidth=1),
)


def styled_fig(fig, title=None, height=400):
    layout = dict(PLOTLY_LAYOUT)
    if title:
        layout["title_text"] = title
    layout["height"] = height
    fig.update_layout(**layout)
    fig.update_xaxes(gridcolor=COLORS["natuurlijk_beige"], gridwidth=0.5)
    fig.update_yaxes(gridcolor=COLORS["natuurlijk_beige"], gridwidth=0.5)
    return fig


def _highlight_cheapest(row, competitors):
    """Highlight de goedkoopste prijs in elke rij groen."""
    styles = [""] * len(row)
    price_cols = ["Westerbergen"] + [c for c in competitors if c in row.index]
    prices = {}
    for col in price_cols:
        val = row.get(col)
        if pd.notna(val) and isinstance(val, (int, float)):
            prices[col] = val

    if prices:
        cheapest = min(prices, key=prices.get)
        for i, col in enumerate(row.index):
            if col == cheapest:
                styles[i] = f"background-color: {COLORS['licht_beige']}; font-weight: 600"
    return styles


# ── Sidebar ─────────────────────────────────────────────────────
with st.sidebar:
    logo_path = os.path.join(_app_dir, "assets", "logo_wit.png")
    if os.path.exists(logo_path):
        st.image(logo_path, width=200)
    st.caption("Pricing Intelligence")
    st.markdown("---")

    db_path = get_db_path()
    if not os.path.exists(db_path):
        st.error("Database niet gevonden.")
        st.stop()

    dates = get_available_dates(db_path)
    if not dates:
        st.warning("Nog geen data. Draai eerst `python run_daily.py`")
        st.stop()

    selected_date = st.selectbox(
        "SCRAPE DATUM", dates, index=0,
        format_func=lambda d: datetime.strptime(d, "%Y-%m-%d").strftime("%d-%m-%Y"),
    )

    analytics = load_analytics(db_path, selected_date)
    meta = analytics.get("metadata", {})
    competitors = meta.get("competitors", sorted(set(
        comp for row in analytics.get("comparison_data", [])
        for comp in row.get("competitors", {}).keys()
    )))

    st.markdown("---")
    selected_competitors = st.multiselect("CONCURRENTEN", competitors, default=competitors)

    all_stay_types = sorted(set(r["stay_type"] for r in analytics.get("comparison_data", [])))
    selected_stay_types = st.multiselect(
        "VERBLIJFSTYPE", all_stay_types, default=all_stay_types,
        format_func=lambda x: STAY_LABELS.get(x, x),
    )

    st.markdown("---")
    st.caption(f"{meta['comparison_count']} vergelijkingen · {len(competitors)} concurrenten")


# ── Data voorbereiden ───────────────────────────────────────────
# Bouw één plat DataFrame van alle comparison_data
rows = []
for row in analytics.get("comparison_data", []):
    if row["stay_type"] not in selected_stay_types:
        continue
    base = {
        "check_in": row["check_in_date"],
        "check_out": row["check_out_date"],
        "nachten": row["nights"],
        "type": STAY_LABELS.get(row["stay_type"], row["stay_type"]),
        "stay_type": row["stay_type"],
        "maand": row["month"],
        "dag": row["day_of_week"],
        "dagen_vooruit": row["days_ahead"],
        "Westerbergen": row["wb_price"],
    }
    has_comp = False
    for comp in selected_competitors:
        info = row.get("competitors", {}).get(comp, {})
        price = info.get("price")
        base[comp] = price
        if price is not None:
            has_comp = True
    if has_comp:
        rows.append(base)

df = pd.DataFrame(rows) if rows else pd.DataFrame()

# Prijsindex data
pi_rows = []
for p in analytics.get("price_index", []):
    if p["stay_type"] not in selected_stay_types:
        continue
    if p["competitor"] not in selected_competitors:
        continue
    pi_rows.append(p)
df_pi = pd.DataFrame(pi_rows) if pi_rows else pd.DataFrame()

# Positie data
pos_rows = []
for p in analytics.get("competitive_position", []):
    if p["stay_type"] not in selected_stay_types:
        continue
    pos_rows.append(p)


# ═══════════════════════════════════════════════════════════════
#  TABS
# ═══════════════════════════════════════════════════════════════
tab_markt, tab_detail, tab_concurrent, tab_systeem = st.tabs([
    "📊 Marktpositie", "💰 Prijsdetail", "🏕️ Per concurrent", "⚙️ Systeem"
])


# ═══════════════════════════════════════════════════════════════
#  TAB 1 — MARKTPOSITIE (overzicht)
# ═══════════════════════════════════════════════════════════════
with tab_markt:

    if df.empty:
        st.info("Geen data voor de huidige filters.")
    else:
        # ── KPI's ───────────────────────────────────────────────────
        avg_index = df_pi["price_index"].mean() if not df_pi.empty else 0
        rank1 = sum(1 for p in pos_rows if p["wb_rank"] == 1)
        total_pos = len(pos_rows)
        rank1_pct = rank1 / total_pos * 100 if total_pos else 0

        avg_wb = df["Westerbergen"].mean()

        comp_prices = []
        for comp in selected_competitors:
            if comp in df.columns:
                comp_prices.extend(df[comp].dropna().tolist())
        avg_markt = sum(comp_prices) / len(comp_prices) if comp_prices else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Gem. prijsindex", f"{avg_index:.0f}", help="< 100 = WB goedkoper dan concurrent")
        c2.metric("Goedkoopst in", f"{rank1_pct:.0f}%", f"{rank1} van {total_pos}")
        c3.metric("Gem. WB prijs", f"€ {avg_wb:,.0f}")
        c4.metric("Gem. marktprijs", f"€ {avg_markt:,.0f}")

        st.markdown("---")

        # ── Prijsindex per concurrent (bar) ─────────────────────────
        col_l, col_r = st.columns([3, 2])

        with col_l:
            if not df_pi.empty:
                avg_by_comp = df_pi.groupby("competitor")["price_index"].mean().sort_values()
                colors = [COLORS["diep_bosgroen"] if v < 85
                          else COLORS["zandgroen"] if v < 115
                          else COLORS["oranje_bruin"] for v in avg_by_comp.values]

                fig = go.Figure(go.Bar(
                    x=avg_by_comp.values, y=avg_by_comp.index, orientation="h",
                    marker_color=colors,
                    text=[f"{v:.0f}" for v in avg_by_comp.values],
                    textposition="auto",
                    textfont=dict(color="#fff", size=13),
                ))
                fig.add_vline(x=100, line_dash="dot", line_color=COLORS["natuurlijk_beige"], line_width=2)
                st.plotly_chart(styled_fig(fig, "Prijsindex per concurrent", max(240, len(avg_by_comp) * 50)),
                                use_container_width=True)

        with col_r:
            if pos_rows:
                rank2 = sum(1 for p in pos_rows if p["wb_rank"] == 2)
                rank3 = total_pos - rank1 - rank2
                fig = go.Figure(go.Pie(
                    labels=["#1 Goedkoopst", "#2", "#3+"],
                    values=[rank1, rank2, rank3], hole=0.65,
                    marker_colors=[COLORS["diep_bosgroen"], COLORS["zandgroen"], COLORS["oranje_bruin"]],
                    textinfo="percent", textfont=dict(size=13, color="#fff"),
                ))
                fig.add_annotation(
                    text=f"<b>{rank1_pct:.0f}%</b><br><span style='font-size:11px;color:{COLORS['tekst_licht']}'>goedkoopst</span>",
                    x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False,
                    font=dict(size=22, family="Playfair Display, serif", color=COLORS["diep_bosgroen"]),
                )
                st.plotly_chart(styled_fig(fig, "Ranking verdeling", 300), use_container_width=True)

        # ── Seizoenslijn ────────────────────────────────────────────
        by_month = analytics.get("seasonal_patterns", {}).get("by_month", {})
        if by_month:
            months = sorted(by_month.keys())
            fig = go.Figure()
            for i, comp in enumerate(selected_competitors):
                prices = [by_month[m].get(comp, {}).get("avg_price") for m in months]
                fig.add_trace(go.Scatter(
                    x=months, y=prices, name=comp, mode="lines+markers",
                    line=dict(color=CHART_COLORS[(i + 1) % len(CHART_COLORS)], width=1.5, dash="dot"),
                    marker=dict(size=5),
                    hovertemplate=f"{comp}: €%{{y:,.0f}}<extra></extra>",
                ))
            wb_prices = [by_month[m].get("Westerbergen", {}).get("avg_price") for m in months]
            fig.add_trace(go.Scatter(
                x=months, y=wb_prices, name="Westerbergen", mode="lines+markers",
                line=dict(color=COLORS["diep_bosgroen"], width=3.5),
                marker=dict(size=8, symbol="diamond"),
                hovertemplate="Westerbergen: €%{y:,.0f}<extra></extra>",
            ))
            st.plotly_chart(styled_fig(fig, "Gemiddelde prijs per maand", 380), use_container_width=True)

        # ── Heatmap: index per week × concurrent ────────────────────
        if not df_pi.empty:
            df_hm = df_pi.copy()
            df_hm["week"] = pd.to_datetime(df_hm["check_in_date"]).dt.isocalendar().week.astype(int)
            df_hm["week_label"] = "W" + df_hm["week"].astype(str)
            pivot = df_hm.pivot_table(values="price_index", index="competitor", columns="week_label", aggfunc="mean")
            pivot = pivot.reindex([c for c in selected_competitors if c in pivot.index])
            cols = sorted(pivot.columns, key=lambda x: int(x[1:]))
            pivot = pivot[cols]

            fig = go.Figure(go.Heatmap(
                z=pivot.values, x=pivot.columns, y=pivot.index,
                colorscale=[[0, COLORS["diep_bosgroen"]], [0.5, COLORS["zandgroen"]], [1, COLORS["oranje_bruin"]]],
                zmin=40, zmax=140,
                text=[[f"{v:.0f}" if pd.notna(v) else "" for v in row] for row in pivot.values],
                texttemplate="%{text}", textfont=dict(size=10),
                hovertemplate="Concurrent: %{y}<br>Week: %{x}<br>Index: %{z:.0f}<extra></extra>",
                colorbar=dict(title=dict(text="Index", side="right")),
            ))
            st.plotly_chart(styled_fig(fig, "Prijsindex per week (< 100 = WB goedkoper)",
                                        max(250, len(pivot) * 45 + 100)), use_container_width=True)


# ═══════════════════════════════════════════════════════════════
#  TAB 2 — PRIJSDETAIL (interactieve tabel)
# ═══════════════════════════════════════════════════════════════
with tab_detail:

    if df.empty:
        st.info("Geen data voor de huidige filters.")
    else:
        st.markdown("## Alle prijzen")

        # Extra filters bovenaan
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            months_available = sorted(df["maand"].unique())
            sel_months = st.multiselect("Maand", months_available, default=months_available, key="detail_month")
        with fc2:
            nights_available = sorted(df["nachten"].unique())
            sel_nights = st.multiselect("Nachten", nights_available, default=nights_available, key="detail_nights")
        with fc3:
            sort_col = st.selectbox("Sorteren op", ["check_in", "Westerbergen"] + selected_competitors, key="detail_sort")

        df_view = df[df["maand"].isin(sel_months) & df["nachten"].isin(sel_nights)].copy()

        if sort_col in df_view.columns:
            df_view = df_view.sort_values(sort_col, na_position="last")

        # Bouw display tabel
        display_cols = ["check_in", "dag", "nachten", "type", "Westerbergen"] + \
                       [c for c in selected_competitors if c in df_view.columns]

        df_display = df_view[display_cols].copy()
        df_display = df_display.rename(columns={"check_in": "Check-in", "dag": "Dag",
                                                  "nachten": "Nachten", "type": "Type"})

        # Format check-in
        df_display["Check-in"] = pd.to_datetime(df_display["Check-in"]).dt.strftime("%d-%m-%Y")
        df_display["Dag"] = df_display["Dag"].str[:2].str.capitalize()

        st.dataframe(
            df_display.style.format(
                {col: "€ {:.0f}" for col in ["Westerbergen"] + selected_competitors if col in df_display.columns},
                na_rep="—",
            ).apply(lambda row: _highlight_cheapest(row, selected_competitors), axis=1),
            use_container_width=True, hide_index=True, height=600,
        )

        st.caption(f"{len(df_display)} rijen weergegeven")

        # Download
        csv = df_view[display_cols].to_csv(index=False, sep=";", decimal=",")
        st.download_button("📥 Download als CSV", csv, f"prijzen_{selected_date}.csv", "text/csv")

        st.markdown("---")

        # ── Prijs scatter over tijd ─────────────────────────────
        st.markdown("### Prijzen over tijd")
        sel_nights_chart = st.selectbox("Verblijfsduur", nights_available, index=0, key="scatter_nights")
        df_scatter = df_view[df_view["nachten"] == sel_nights_chart].copy()
        df_scatter["check_in_dt"] = pd.to_datetime(df_scatter["check_in"])

        if not df_scatter.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_scatter["check_in_dt"], y=df_scatter["Westerbergen"],
                name="Westerbergen", mode="lines+markers",
                line=dict(color=COLORS["diep_bosgroen"], width=3),
                marker=dict(size=6),
                hovertemplate="WB: €%{y:,.0f}<br>%{x|%d-%m-%Y}<extra></extra>",
            ))
            for i, comp in enumerate(selected_competitors):
                if comp in df_scatter.columns:
                    fig.add_trace(go.Scatter(
                        x=df_scatter["check_in_dt"], y=df_scatter[comp],
                        name=comp, mode="lines+markers",
                        line=dict(color=CHART_COLORS[(i + 1) % len(CHART_COLORS)], width=1.5, dash="dot"),
                        marker=dict(size=4),
                        hovertemplate=f"{comp}: €%{{y:,.0f}}<extra></extra>",
                    ))
            st.plotly_chart(styled_fig(fig, f"Prijzen {sel_nights_chart} nachten", 400), use_container_width=True)


# ═══════════════════════════════════════════════════════════════
#  TAB 3 — PER CONCURRENT (deep-dive)
# ═══════════════════════════════════════════════════════════════
with tab_concurrent:

    if df.empty or not selected_competitors:
        st.info("Geen data voor de huidige filters.")
    else:
        # ── Profiel overzicht ───────────────────────────────────
        st.markdown("## Concurrentenprofiel")

        profile_rows = []
        for comp in selected_competitors:
            comp_pi = df_pi[df_pi["competitor"] == comp] if not df_pi.empty else pd.DataFrame()
            if not comp_pi.empty:
                profile_rows.append({
                    "Concurrent": comp,
                    "Gem. prijs": f"€ {comp_pi['comp_price'].mean():,.0f}",
                    "Min": f"€ {comp_pi['comp_price'].min():,.0f}",
                    "Max": f"€ {comp_pi['comp_price'].max():,.0f}",
                    "Gem. index": f"{comp_pi['price_index'].mean():.0f}",
                    "Datapunten": len(comp_pi),
                })

        if profile_rows:
            st.dataframe(pd.DataFrame(profile_rows), use_container_width=True, hide_index=True)

        st.markdown("---")

        # ── Head-to-head ────────────────────────────────────────
        st.markdown("## Head-to-head")
        selected_comp = st.selectbox("Kies concurrent", selected_competitors, key="h2h")

        if selected_comp and selected_comp in df.columns:
            df_h2h = df[["check_in", "nachten", "type", "Westerbergen", selected_comp]].dropna(
                subset=[selected_comp]
            ).copy()

            if not df_h2h.empty:
                df_h2h["verschil"] = df_h2h["Westerbergen"] - df_h2h[selected_comp]
                df_h2h["index"] = (df_h2h["Westerbergen"] / df_h2h[selected_comp] * 100).round(0)

                # KPIs
                kc1, kc2, kc3, kc4 = st.columns(4)
                avg_diff = df_h2h["verschil"].mean()
                wb_goedkoper = (df_h2h["verschil"] < 0).sum()
                wb_duurder = (df_h2h["verschil"] > 0).sum()
                avg_idx = df_h2h["index"].mean()

                kc1.metric("Gem. verschil", f"€ {avg_diff:+,.0f}")
                kc2.metric("WB goedkoper", f"{wb_goedkoper}x")
                kc3.metric("WB duurder", f"{wb_duurder}x")
                kc4.metric("Gem. index", f"{avg_idx:.0f}")

                # Scatter plot
                max_p = max(df_h2h["Westerbergen"].max(), df_h2h[selected_comp].max()) * 1.05
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=[0, max_p], y=[0, max_p], mode="lines",
                    line=dict(color=COLORS["natuurlijk_beige"], dash="dash", width=1.5),
                    showlegend=False, hoverinfo="skip",
                ))

                df_h2h["color"] = df_h2h["index"].apply(
                    lambda x: COLORS["diep_bosgroen"] if x < 85
                    else COLORS["zandgroen"] if x < 115
                    else COLORS["oranje_bruin"]
                )

                fig.add_trace(go.Scatter(
                    x=df_h2h[selected_comp], y=df_h2h["Westerbergen"],
                    mode="markers",
                    marker=dict(color=df_h2h["color"], size=9,
                                line=dict(width=0.5, color="#fff")),
                    text=df_h2h.apply(lambda r: f"{r['check_in']}, {r['nachten']}n", axis=1),
                    hovertemplate="WB: €%{y:,.0f}<br>" + selected_comp + ": €%{x:,.0f}<br>%{text}<extra></extra>",
                    showlegend=False,
                ))
                fig.update_xaxes(title_text=f"{selected_comp} (€)")
                fig.update_yaxes(title_text="Westerbergen (€)")
                fig.add_annotation(text="Boven lijn = WB duurder", x=0.97, y=0.03,
                                   xref="paper", yref="paper", showarrow=False,
                                   font=dict(size=10, color=COLORS["tekst_licht"]))
                st.plotly_chart(styled_fig(fig, f"Westerbergen vs {selected_comp}", 420),
                                use_container_width=True)

                # Detail tabel
                df_detail = df_h2h.copy()
                df_detail["Check-in"] = pd.to_datetime(df_detail["check_in"]).dt.strftime("%d-%m-%Y")
                df_show = df_detail[["Check-in", "nachten", "type", "Westerbergen",
                                      selected_comp, "verschil", "index"]].rename(columns={
                    "nachten": "Nachten", "type": "Type", "verschil": "Verschil €", "index": "Index"
                })
                st.dataframe(
                    df_show.style.format({
                        "Westerbergen": "€ {:.0f}", selected_comp: "€ {:.0f}",
                        "Verschil €": "€ {:+,.0f}", "Index": "{:.0f}",
                    }),
                    use_container_width=True, hide_index=True, height=400,
                )
            else:
                st.info(f"Geen overlappende data met {selected_comp}.")


# ═══════════════════════════════════════════════════════════════
#  TAB 4 — SYSTEEM
# ═══════════════════════════════════════════════════════════════
with tab_systeem:
    st.markdown("## Systeem")

    # Scrape status
    scrape_status = get_scrape_status(db_path, selected_date)
    if scrape_status:
        st.markdown("### Scrape-status")
        status_rows = []
        for name, info in sorted(scrape_status.items()):
            s = info.get("status", "?")
            emoji = "✅" if s == "success" else "❌" if s == "failed" else "⚠️"
            status_rows.append({
                "": emoji, "Scraper": name, "Status": s.capitalize(),
                "Records": info.get("records_scraped", 0),
                "Duur": f"{info.get('duration_seconds', 0):.0f}s",
            })
        st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)

    st.markdown("---")

    # Datadekking
    st.markdown("### Datadekking per concurrent")
    total = len(analytics.get("comparison_data", []))
    cov = {}
    for row in analytics.get("comparison_data", []):
        for comp, info in row.get("competitors", {}).items():
            if info.get("price") is not None:
                cov[comp] = cov.get(comp, 0) + 1

    cov_rows = []
    for comp in sorted(cov.keys()):
        cnt = cov[comp]
        pct = cnt / total * 100 if total else 0
        cov_rows.append({"Concurrent": comp, "Prijzen": cnt, "Dekking": f"{pct:.0f}%",
                          "": "█" * int(pct / 5) + "░" * (20 - int(pct / 5))})
    st.dataframe(pd.DataFrame(cov_rows), use_container_width=True, hide_index=True)

    st.markdown("---")

    # DB info
    c1, c2, c3 = st.columns(3)
    c1.metric("Scrape-datums", len(dates))
    db_mb = os.path.getsize(db_path) / (1024 * 1024) if os.path.exists(db_path) else 0
    c2.metric("Database", f"{db_mb:.1f} MB")
    c3.metric("Vergelijkingen", meta.get("comparison_count", 0))

    # Prijswijzigingen
    changes = analytics.get("price_changes", {})
    st.markdown("---")
    st.markdown("### Prijswijzigingen")
    if changes.get("status") == "onvoldoende_data":
        st.info("Prijshistorie wordt opgebouwd zodra er meerdere scrape-dagen zijn.")
    elif changes.get("changes"):
        st.metric("Wijzigingen gedetecteerd", changes["total_changes"])
        ch_rows = []
        for c in changes["changes"]:
            em = "📈" if c["price_change"] > 0 else "📉"
            ch_rows.append({
                "": em, "Concurrent": c["competitor_name"],
                "Check-in": datetime.strptime(c["check_in_date"], "%Y-%m-%d").strftime("%d-%m-%Y"),
                "Was": f"€ {c['prev_price']:,.0f}", "Nu": f"€ {c['curr_price']:,.0f}",
                "Verschil": f"€ {c['price_change']:+,.0f}", "%": f"{c['change_pct']:+.1f}%",
            })
        st.dataframe(pd.DataFrame(ch_rows), use_container_width=True, hide_index=True)
    else:
        st.info("Geen prijswijzigingen gedetecteerd.")

    st.markdown("---")
    if st.button("🔄 Herbereken analytics"):
        load_analytics.clear()
        st.rerun()

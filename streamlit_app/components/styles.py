"""Westerbergen Pricing Intelligence — Brand Styling & CSS.

Gebaseerd op Westerbergen Brandbook 2025 + Crextio SaaS-esthetiek.
"""

# ── Brand Colors ────────────────────────────────────────────────
COLORS = {
    # Primair (brandbook)
    "diep_bosgroen": "#3B4E37",
    "zandgroen": "#A7A158",
    "natuurlijk_beige": "#E2D6C8",
    "heide_paars": "#AE60A2",
    # Secundair (brandbook)
    "warm_goud": "#8D6828",
    "schors_bruin": "#60443A",
    "mos_groen": "#9F9368",
    "oranje_bruin": "#C67741",
    # UI helpers
    "wit": "#FFFFFF",
    "licht_beige": "#F5F0EB",
    "tekst_donker": "#2C3E28",
    "tekst_licht": "#6B7B67",
}

# Semafoor
DATA_COLORS = {
    "goed": "#C6EFCE",
    "neutraal": "#FFEB9C",
    "slecht": "#FFC7CE",
}

# Plotly volgorde — Westerbergen altijd eerst
CHART_COLORS = [
    "#3B4E37", "#A7A158", "#AE60A2", "#9F9368",
    "#C67741", "#8D6828", "#60443A",
]

STAY_LABELS = {
    "weekend": "Weekend (vr-zo)",
    "midweek": "Midweek (ma-vr)",
    "week": "Week (vr-vr)",
    "lang_weekend": "Lang weekend (3n)",
    "kort_verblijf_2n": "Kort verblijf (2n)",
    "kort_verblijf_4n": "Kort verblijf (4n)",
    "week_overig": "Week overig (7n)",
}


def get_custom_css() -> str:
    """Return volledige custom CSS string."""
    return f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,700;1,400&family=Inter:wght@300;400;500;600;700&display=swap');

        /* ── Global ── */
        .stApp {{
            background-color: {COLORS['licht_beige']};
        }}

        .main .block-container {{
            padding-top: 1.5rem;
            max-width: 1300px;
        }}

        /* ── Typography ── */
        h1, h2, h3 {{
            font-family: 'Playfair Display', Georgia, serif !important;
            color: {COLORS['diep_bosgroen']} !important;
        }}

        h1 {{ font-size: 1.9rem !important; font-weight: 700 !important; }}
        h2 {{ font-size: 1.4rem !important; font-weight: 700 !important; }}
        h3 {{ font-size: 1.1rem !important; font-weight: 600 !important; }}

        p, span, label, .stMarkdown, div {{
            font-family: 'Inter', Helvetica, Arial, sans-serif !important;
        }}

        /* ── Sidebar ── */
        section[data-testid="stSidebar"] {{
            background-color: {COLORS['diep_bosgroen']} !important;
        }}

        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] .stMarkdown {{
            color: {COLORS['wit']} !important;
        }}

        section[data-testid="stSidebar"] .stSelectbox label,
        section[data-testid="stSidebar"] .stMultiSelect label {{
            color: {COLORS['natuurlijk_beige']} !important;
            font-size: 0.8rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.5px !important;
        }}

        /* ── KPI Metric Cards ── */
        div[data-testid="stMetric"] {{
            background: {COLORS['wit']};
            border-radius: 12px;
            padding: 16px 20px;
            box-shadow: 0 1px 6px rgba(59, 78, 55, 0.08);
            border-left: 4px solid {COLORS['diep_bosgroen']};
        }}

        div[data-testid="stMetric"] label {{
            font-family: 'Inter', Helvetica, sans-serif !important;
            color: {COLORS['tekst_licht']} !important;
            font-size: 0.72rem !important;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
            font-family: 'Playfair Display', Georgia, serif !important;
            color: {COLORS['diep_bosgroen']} !important;
            font-size: 1.9rem !important;
        }}

        /* ── Tabs ── */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 6px;
            border-bottom: 2px solid {COLORS['natuurlijk_beige']};
        }}

        .stTabs [data-baseweb="tab"] {{
            background-color: transparent;
            border-radius: 8px 8px 0 0;
            padding: 8px 20px;
            font-family: 'Inter', Helvetica, sans-serif;
            color: {COLORS['tekst_licht']};
            font-weight: 500;
            font-size: 0.9rem;
        }}

        .stTabs [aria-selected="true"] {{
            background-color: {COLORS['diep_bosgroen']} !important;
            color: {COLORS['wit']} !important;
            font-weight: 600;
        }}

        /* ── Buttons ── */
        .stButton > button {{
            background-color: {COLORS['diep_bosgroen']};
            color: {COLORS['wit']};
            border: none;
            border-radius: 8px;
            font-family: 'Inter', Helvetica, sans-serif;
            font-weight: 500;
            padding: 8px 24px;
            transition: all 0.2s;
        }}

        .stButton > button:hover {{
            background-color: {COLORS['zandgroen']};
            color: {COLORS['wit']};
        }}

        .stDownloadButton > button {{
            background-color: {COLORS['zandgroen']};
            color: {COLORS['wit']};
            border: none;
            border-radius: 8px;
        }}

        .stDownloadButton > button:hover {{
            background-color: {COLORS['diep_bosgroen']};
        }}

        /* ── DataFrames ── */
        .stDataFrame {{
            border-radius: 12px;
            overflow: hidden;
        }}

        /* ── Expander ── */
        .streamlit-expanderHeader {{
            background-color: {COLORS['wit']};
            border-radius: 8px;
            font-family: 'Inter', Helvetica, sans-serif;
            color: {COLORS['diep_bosgroen']};
        }}

        /* ── Divider ── */
        hr {{
            border-color: {COLORS['natuurlijk_beige']};
        }}

        /* ── Card wrapper (custom HTML) ── */
        .wb-card {{
            background: {COLORS['wit']};
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 1px 6px rgba(59, 78, 55, 0.08);
            margin-bottom: 16px;
        }}

        .wb-card-title {{
            font-family: 'Playfair Display', Georgia, serif;
            font-size: 1.1rem;
            color: {COLORS['diep_bosgroen']};
            margin-bottom: 12px;
            font-weight: 600;
        }}

        /* ── Urgentie badges ── */
        .badge-hoog {{
            background-color: {DATA_COLORS['slecht']};
            color: #9C2D2D;
            padding: 3px 12px;
            border-radius: 12px;
            font-size: 0.78rem;
            font-weight: 600;
        }}

        .badge-middel {{
            background-color: {DATA_COLORS['neutraal']};
            color: #8B6914;
            padding: 3px 12px;
            border-radius: 12px;
            font-size: 0.78rem;
            font-weight: 600;
        }}

        .badge-laag {{
            background-color: {DATA_COLORS['goed']};
            color: #1E6B31;
            padding: 3px 12px;
            border-radius: 12px;
            font-size: 0.78rem;
            font-weight: 600;
        }}

        /* ── Hide Streamlit branding ── */
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
    </style>
    """


def urgentie_badge(level: str) -> str:
    """Return HTML badge voor urgentie-niveau."""
    labels = {"hoog": "Hoog", "middel": "Middel", "laag": "Laag"}
    return f'<span class="badge-{level}">{labels.get(level, level)}</span>'


def card_start(title: str = "") -> str:
    """Open een dashboard card (HTML)."""
    title_html = f'<div class="wb-card-title">{title}</div>' if title else ""
    return f'<div class="wb-card">{title_html}'


def card_end() -> str:
    """Sluit een dashboard card."""
    return '</div>'

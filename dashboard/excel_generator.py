"""Excel Dashboard generator using xlsxwriter."""

import xlsxwriter
from collections import defaultdict
from datetime import datetime

# Color palette
WB_DARK_GREEN = '#2E5A1C'
WB_GREEN = '#4A8C2A'
WB_LIGHT_GREEN = '#E8F5E0'
WB_ACCENT = '#F5A623'
WB_WHITE = '#FFFFFF'
WB_DARK_GRAY = '#333333'
WB_LIGHT_GRAY = '#F5F5F5'

CF_GREEN = '#C6EFCE'
CF_YELLOW = '#FFEB9C'
CF_RED = '#FFC7CE'

STAY_LABELS = {
    "weekend": "Weekend (vr-zo)",
    "midweek": "Midweek (ma-vr)",
    "week": "Week (vr-vr)",
    "lang_weekend": "Lang weekend (3n)",
    "kort_verblijf_2n": "Kort verblijf (2n)",
    "kort_verblijf_4n": "Kort verblijf (4n)",
    "week_overig": "Week overig (7n)",
}


class ExcelDashboard:
    """Generate a professional Excel dashboard from analytics results."""

    def __init__(self, analytics_result: dict, config: dict = None):
        self.data = analytics_result
        self.config = config or {}
        self.workbook = None
        self._formats = {}
        self.competitors = sorted(self.data.get("metadata", {}).get("competitors", []))

    def generate(self, output_path: str) -> str:
        """Create the workbook, write all sheets, close, return path."""
        self.workbook = xlsxwriter.Workbook(output_path)
        try:
            self._init_formats()

            self._write_overzicht()
            self._write_prijsvergelijking()
            self._write_concurrenten()
            self._write_historisch()
        finally:
            self.workbook.close()
        return output_path

    # ── Format initialization ──────────────────────────────────────────

    def _init_formats(self):
        wb = self.workbook
        f = {}

        f["title"] = wb.add_format({
            "bold": True, "font_size": 16, "font_color": WB_DARK_GREEN,
            "bottom": 2, "bottom_color": WB_GREEN,
        })
        f["subtitle"] = wb.add_format({
            "bold": True, "font_size": 12, "font_color": WB_GREEN,
        })
        f["info"] = wb.add_format({
            "font_size": 10, "font_color": "#666666", "italic": True,
        })

        # Table headers
        f["header"] = wb.add_format({
            "bold": True, "font_size": 10, "font_color": WB_WHITE,
            "bg_color": WB_DARK_GREEN, "border": 1, "border_color": WB_GREEN,
            "text_wrap": True, "valign": "vcenter",
        })
        f["header_right"] = wb.add_format({
            "bold": True, "font_size": 10, "font_color": WB_WHITE,
            "bg_color": WB_DARK_GREEN, "border": 1, "border_color": WB_GREEN,
            "text_wrap": True, "valign": "vcenter", "align": "right",
        })

        # Body text
        for suffix, bg in [("", None), ("_alt", WB_LIGHT_GREEN)]:
            props = {"font_size": 10, "font_color": WB_DARK_GRAY, "border": 1, "border_color": "#DDDDDD"}
            if bg:
                props["bg_color"] = bg

            f[f"text{suffix}"] = wb.add_format(props)
            f[f"text_bold{suffix}"] = wb.add_format({**props, "bold": True})

            f[f"euro{suffix}"] = wb.add_format({**props, "num_format": '#,##0', "align": "right"})
            f[f"euro_eur{suffix}"] = wb.add_format({**props, "num_format": u'\u20ac #,##0', "align": "right"})
            f[f"euro_bold{suffix}"] = wb.add_format({**props, "num_format": u'\u20ac #,##0', "bold": True, "align": "right"})

            f[f"index{suffix}"] = wb.add_format({**props, "num_format": "0.0", "align": "center"})
            f[f"pct{suffix}"] = wb.add_format({**props, "num_format": "0.0%", "align": "center"})
            f[f"int{suffix}"] = wb.add_format({**props, "num_format": "0", "align": "center"})
            f[f"date{suffix}"] = wb.add_format({**props, "num_format": "dd-mm-yyyy", "align": "left"})

        # Text centered
        f["text_center"] = wb.add_format({
            "font_size": 10, "font_color": WB_DARK_GRAY, "border": 1,
            "border_color": "#DDDDDD", "align": "center",
        })
        f["text_center_alt"] = wb.add_format({
            "font_size": 10, "font_color": WB_DARK_GRAY, "border": 1,
            "border_color": "#DDDDDD", "align": "center", "bg_color": WB_LIGHT_GREEN,
        })

        # Unavailable / no data
        f["na"] = wb.add_format({
            "font_size": 10, "font_color": "#AAAAAA", "italic": True,
            "border": 1, "border_color": "#DDDDDD", "align": "center",
        })
        f["na_alt"] = wb.add_format({
            "font_size": 10, "font_color": "#AAAAAA", "italic": True,
            "border": 1, "border_color": "#DDDDDD", "align": "center",
            "bg_color": WB_LIGHT_GREEN,
        })

        # KPI cards
        f["kpi_value"] = wb.add_format({
            "bold": True, "font_size": 22, "font_color": WB_DARK_GREEN,
            "align": "center", "valign": "vcenter",
            "border": 2, "border_color": WB_GREEN,
        })
        f["kpi_label"] = wb.add_format({
            "font_size": 9, "font_color": WB_GREEN,
            "align": "center", "valign": "vcenter",
            "border": 2, "border_color": WB_GREEN, "text_wrap": True,
        })

        # Urgency
        f["urgentie_hoog"] = wb.add_format({
            "font_size": 10, "bold": True, "font_color": "#9C0006",
            "bg_color": CF_RED, "border": 1, "border_color": "#DDDDDD", "align": "center",
        })
        f["urgentie_middel"] = wb.add_format({
            "font_size": 10, "bold": True, "font_color": "#9C6500",
            "bg_color": CF_YELLOW, "border": 1, "border_color": "#DDDDDD", "align": "center",
        })
        f["urgentie_laag"] = wb.add_format({
            "font_size": 10, "font_color": "#006100",
            "bg_color": CF_GREEN, "border": 1, "border_color": "#DDDDDD", "align": "center",
        })

        # Recommendation type
        f["type_verhoging"] = wb.add_format({
            "font_size": 10, "bold": True, "font_color": "#006100",
            "border": 1, "border_color": "#DDDDDD", "align": "center",
        })
        f["type_verlaging"] = wb.add_format({
            "font_size": 10, "bold": True, "font_color": "#9C0006",
            "border": 1, "border_color": "#DDDDDD", "align": "center",
        })

        # Total/summary row
        f["total"] = wb.add_format({
            "bold": True, "font_size": 10, "font_color": WB_DARK_GREEN,
            "top": 2, "top_color": WB_GREEN, "border": 1, "border_color": "#DDDDDD",
        })
        f["total_euro"] = wb.add_format({
            "bold": True, "font_size": 10, "font_color": WB_DARK_GREEN,
            "num_format": u'\u20ac #,##0', "align": "right",
            "top": 2, "top_color": WB_GREEN, "border": 1, "border_color": "#DDDDDD",
        })
        f["total_index"] = wb.add_format({
            "bold": True, "font_size": 10, "font_color": WB_DARK_GREEN,
            "num_format": "0.0", "align": "center",
            "top": 2, "top_color": WB_GREEN, "border": 1, "border_color": "#DDDDDD",
        })
        f["total_int"] = wb.add_format({
            "bold": True, "font_size": 10, "font_color": WB_DARK_GREEN,
            "num_format": "0", "align": "center",
            "top": 2, "top_color": WB_GREEN, "border": 1, "border_color": "#DDDDDD",
        })

        self._formats = f

    # ── Helpers ─────────────────────────────────────────────────────────

    def _fmt(self, name: str, alt: bool = False):
        """Get format, using _alt variant if alt=True."""
        key = f"{name}_alt" if alt else name
        return self._formats.get(key, self._formats.get(name))

    def _write_section_header(self, ws, row: int, title: str) -> int:
        ws.write(row, 0, title, self._formats["subtitle"])
        return row + 1

    def _set_col_widths(self, ws, widths: list):
        for i, w in enumerate(widths):
            ws.set_column(i, i, w)

    # ── Sheet 1: Overzicht ─────────────────────────────────────────────

    def _write_overzicht(self):
        ws = self.workbook.add_worksheet("Overzicht")
        ws.set_tab_color(WB_DARK_GREEN)
        f = self._formats
        meta = self.data["metadata"]

        # Title
        ws.merge_range("A1:H1", "CONCURRENTIECHECK WESTERBERGEN", f["title"])
        ws.write(1, 0, f"Rapport datum: {datetime.now().strftime('%d-%m-%Y')}  |  "
                       f"Scrape: {meta['scrape_date']}  |  "
                       f"{meta['comparison_count']} vergelijkingen", f["info"])

        # ── KPI Cards ──
        row = 3
        ws.write(row, 0, "KERNGETALLEN", f["subtitle"])
        row += 1

        # Compute KPI values
        pi_data = self.data.get("price_index", [])
        pos_data = self.data.get("competitive_position", [])
        recs = self.data.get("recommendations", [])
        ppn_data = self.data.get("price_per_night", [])
        gaps = self.data.get("availability_gaps", {})

        avg_index = sum(p["price_index"] for p in pi_data) / len(pi_data) if pi_data else 0
        pct_cheapest = sum(1 for p in pos_data if p["wb_rank"] == 1) / len(pos_data) * 100 if pos_data else 0
        verhogingen = [r for r in recs if r["type"] == "verhoging"]
        extra_omzet = sum(r.get("extra_omzet", 0) for r in verhogingen if r.get("extra_omzet", 0) > 0)
        avg_ppn = sum(p["wb_ppn"] for p in ppn_data) / len(ppn_data) if ppn_data else 0
        uitverkocht = gaps.get("summary", {}).get("raise_opportunity_count", 0)
        acties_hoog = sum(1 for r in recs if r.get("urgentie") == "hoog")

        kpis = [
            ("Gem. Prijsindex", f"{avg_index:.1f}", "< 100 = goedkoper"),
            ("Goedkoopst in", f"{pct_cheapest:.0f}%", "van alle vergelijkingen"),
            ("Verhogingspotentieel", f"\u20ac {extra_omzet:,.0f}", "geschatte extra omzet"),
            ("Gem. prijs/nacht", f"\u20ac {avg_ppn:.0f}", "Westerbergen"),
            ("Conc. uitverkocht", str(uitverkocht), "combinaties"),
            ("Acties nodig", str(acties_hoog), "hoge urgentie"),
        ]

        for i, (label, value, desc) in enumerate(kpis):
            col = (i % 3) * 3
            r = row + (i // 3) * 3
            ws.merge_range(r, col, r, col + 1, value, f["kpi_value"])
            ws.merge_range(r + 1, col, r + 1, col + 1, f"{label}\n{desc}", f["kpi_label"])

        row += 7

        # ── Prijsindex per concurrent ──
        row = self._write_section_header(ws, row, "PRIJSINDEX PER CONCURRENT")
        row += 1

        headers = ["Concurrent", "Gem. Index", "Min", "Max", "Vergelijkingen"]
        widths = [22, 12, 10, 10, 14]
        self._set_col_widths(ws, widths)

        for c, h in enumerate(headers):
            ws.write(row, c, h, f["header"] if c == 0 else f["header_right"])
        row += 1

        by_comp = defaultdict(list)
        for pi in pi_data:
            by_comp[pi["competitor"]].append(pi["price_index"])

        for i, comp in enumerate(sorted(by_comp.keys())):
            indices = by_comp[comp]
            alt = i % 2 == 1
            avg = sum(indices) / len(indices)
            ws.write(row, 0, comp, self._fmt("text", alt))
            ws.write(row, 1, avg, self._fmt("index", alt))
            ws.write(row, 2, min(indices), self._fmt("index", alt))
            ws.write(row, 3, max(indices), self._fmt("index", alt))
            ws.write(row, 4, len(indices), self._fmt("int", alt))
            row += 1

        # Total row
        all_indices = [p["price_index"] for p in pi_data]
        if all_indices:
            ws.write(row, 0, "TOTAAL", f["total"])
            ws.write(row, 1, sum(all_indices) / len(all_indices), f["total_index"])
            ws.write(row, 2, min(all_indices), f["total_index"])
            ws.write(row, 3, max(all_indices), f["total_index"])
            ws.write(row, 4, len(all_indices), f["total_int"])
        row += 2

        # ── Concurrentiepositie ──
        row = self._write_section_header(ws, row, "CONCURRENTIEPOSITIE")
        row += 1

        for c, h in enumerate(["Positie", "Aantal keren", "Percentage"]):
            ws.write(row, c, h, f["header"] if c == 0 else f["header_right"])
        row += 1

        rank_counts = defaultdict(int)
        for pos in pos_data:
            rank_counts[min(pos["wb_rank"], 3)] += 1

        total_pos = len(pos_data) or 1
        for i, (rank_label, rank_key) in enumerate([(1, 1), (2, 2), ("3+", 3)]):
            alt = i % 2 == 1
            count = rank_counts.get(rank_key, 0)
            ws.write(row, 0, f"#{rank_label}", self._fmt("text", alt))
            ws.write(row, 1, count, self._fmt("int", alt))
            ws.write(row, 2, count / total_pos, self._fmt("pct", alt))
            row += 1
        row += 1

        # ── Top 5 adviezen ──
        row = self._write_section_header(ws, row, "TOP 5 PRIJSADVIEZEN")
        row += 1

        adv_headers = ["Check-in", "Nachten", "Type", "Urgentie", "Huidig", "Voorgesteld", "Reden"]
        adv_widths = [12, 9, 12, 11, 12, 13, 50]
        self._set_col_widths(ws, adv_widths)

        for c, h in enumerate(adv_headers):
            ws.write(row, c, h, f["header"])
        row += 1

        for i, rec in enumerate(recs[:5]):
            alt = i % 2 == 1
            ws.write(row, 0, rec["check_in_date"], self._fmt("text", alt))
            ws.write(row, 1, f"{rec['nights']}n", self._fmt("text_center", alt))

            type_fmt = f["type_verhoging"] if rec["type"] == "verhoging" else f["type_verlaging"]
            ws.write(row, 2, rec["type"].upper(), type_fmt)

            urg_fmt = f.get(f"urgentie_{rec['urgentie']}", self._fmt("text", alt))
            ws.write(row, 3, rec["urgentie"], urg_fmt)

            ws.write(row, 4, rec["huidig_prijs"], self._fmt("euro_eur", alt))
            if rec.get("voorgesteld_prijs"):
                ws.write(row, 5, rec["voorgesteld_prijs"], self._fmt("euro_eur", alt))
            else:
                ws.write(row, 5, "-", self._fmt("na", alt))

            reden = rec.get("reden", "")
            if len(reden) > 80:
                reden = reden[:77] + "..."
            ws.write(row, 6, reden, self._fmt("text", alt))
            row += 1

        # Print setup
        ws.set_landscape()
        ws.set_paper(9)  # A4
        ws.fit_to_pages(1, 0)

    # ── Sheet 2: Prijsvergelijking ─────────────────────────────────────

    def _write_prijsvergelijking(self):
        ws = self.workbook.add_worksheet("Prijsvergelijking")
        ws.set_tab_color(WB_GREEN)
        f = self._formats

        ws.merge_range("A1:F1", "PRIJSVERGELIJKING", f["title"])
        ws.write(1, 0, "Prijzen per check-in datum en verblijfsduur", f["info"])

        row = 3

        # Build headers: fixed cols + 2 per competitor + summary
        headers = ["Check-in", "Dag", "Nachten", "Type", "WB Prijs"]
        col_widths = [12, 11, 9, 16, 12]

        for comp in self.competitors:
            short = comp[:16]
            headers.append(f"{short} Prijs")
            headers.append(f"{short} Index")
            col_widths.extend([13, 10])

        headers.extend(["Gem. Index", "WB Rang"])
        col_widths.extend([11, 9])

        self._set_col_widths(ws, col_widths)

        for c, h in enumerate(headers):
            ws.write(row, c, h, f["header"])

        header_row = row
        row += 1

        # Build lookup for price_index data
        pi_lookup = {}
        for pi in self.data.get("price_index", []):
            key = (pi["check_in_date"], pi["nights"], pi["competitor"])
            pi_lookup[key] = pi

        # Build lookup for position data
        pos_lookup = {}
        for pos in self.data.get("competitive_position", []):
            key = (pos["check_in_date"], pos["nights"])
            pos_lookup[key] = pos

        # Index columns for conditional formatting
        index_cols = []
        for i, comp in enumerate(self.competitors):
            index_cols.append(5 + i * 2 + 1)  # The "Index" column for each competitor
        avg_index_col = 5 + len(self.competitors) * 2
        rank_col = avg_index_col + 1

        data_start_row = row
        comparison_data = self.data.get("comparison_data", [])

        for i, cd in enumerate(comparison_data):
            alt = i % 2 == 1
            col = 0

            ws.write(row, col, cd["check_in_date"], self._fmt("text", alt)); col += 1
            ws.write(row, col, cd["day_of_week"], self._fmt("text", alt)); col += 1
            ws.write(row, col, cd["nights"], self._fmt("int", alt)); col += 1
            ws.write(row, col, STAY_LABELS.get(cd["stay_type"], cd["stay_type"]), self._fmt("text", alt)); col += 1
            ws.write(row, col, cd["wb_price"], self._fmt("euro", alt)); col += 1

            indices_this_row = []
            for comp in self.competitors:
                comp_data = cd["competitors"].get(comp)
                if comp_data and comp_data["price"] and comp_data["available"]:
                    ws.write(row, col, comp_data["price"], self._fmt("euro", alt))
                    pi_key = (cd["check_in_date"], cd["nights"], comp)
                    pi = pi_lookup.get(pi_key)
                    if pi:
                        ws.write(row, col + 1, pi["price_index"], self._fmt("index", alt))
                        indices_this_row.append(pi["price_index"])
                    else:
                        ws.write(row, col + 1, "-", self._fmt("na", alt))
                else:
                    ws.write(row, col, "-", self._fmt("na", alt))
                    ws.write(row, col + 1, "-", self._fmt("na", alt))
                col += 2

            # Average index
            if indices_this_row:
                ws.write(row, col, sum(indices_this_row) / len(indices_this_row), self._fmt("index", alt))
            else:
                ws.write(row, col, "-", self._fmt("na", alt))
            col += 1

            # WB rank
            pos = pos_lookup.get((cd["check_in_date"], cd["nights"]))
            if pos:
                ws.write(row, col, pos["wb_rank"], self._fmt("int", alt))
            else:
                ws.write(row, col, "-", self._fmt("na", alt))

            row += 1

        data_end_row = row - 1

        # Autofilter
        if data_end_row >= data_start_row:
            ws.autofilter(header_row, 0, data_end_row, len(headers) - 1)

        # Conditional formatting on index columns
        for ic in index_cols + [avg_index_col]:
            if data_end_row >= data_start_row:
                ws.conditional_format(data_start_row, ic, data_end_row, ic, {
                    "type": "3_color_scale",
                    "min_color": CF_GREEN, "min_type": "num", "min_value": 30,
                    "mid_color": CF_YELLOW, "mid_type": "num", "mid_value": 100,
                    "max_color": CF_RED, "max_type": "num", "max_value": 130,
                })

        # Freeze panes
        ws.freeze_panes(header_row + 1, 5)

        # Print setup
        ws.set_landscape()
        ws.set_paper(9)
        ws.fit_to_pages(1, 0)
        ws.repeat_rows(header_row)

    # ── Sheet 3: Concurrenten ──────────────────────────────────────────

    def _write_concurrenten(self):
        ws = self.workbook.add_worksheet("Concurrenten")
        ws.set_tab_color('#4472C4')
        f = self._formats

        ws.merge_range("A1:F1", "CONCURRENTENANALYSE", f["title"])
        ws.write(1, 0, "Seizoenspatronen en prijsniveaus per concurrent", f["info"])

        row = 3
        seasonal = self.data.get("seasonal_patterns", {})

        # ── Section A: Gemiddelde prijs per maand ──
        row = self._write_section_header(ws, row, "GEMIDDELDE PRIJS PER MAAND")
        row += 1

        by_month = seasonal.get("by_month", {})

        month_headers = ["Maand", "Westerbergen"]
        for comp in self.competitors:
            month_headers.append(comp[:16])
        month_headers.append("Gem. Concurrent")
        widths = [12, 14] + [14] * len(self.competitors) + [15]
        self._set_col_widths(ws, widths)

        for c, h in enumerate(month_headers):
            ws.write(row, c, h, f["header"])
        header_row = row
        row += 1

        data_start = row
        for i, month in enumerate(sorted(by_month.keys())):
            alt = i % 2 == 1
            comps_data = by_month[month]
            ws.write(row, 0, month, self._fmt("text", alt))

            wb_data = comps_data.get("Westerbergen")
            if wb_data:
                ws.write(row, 1, wb_data["avg_price"], self._fmt("euro_eur", alt))
            else:
                ws.write(row, 1, "-", self._fmt("na", alt))

            comp_avgs = []
            for j, comp in enumerate(self.competitors):
                cd = comps_data.get(comp)
                if cd:
                    ws.write(row, 2 + j, cd["avg_price"], self._fmt("euro_eur", alt))
                    comp_avgs.append(cd["avg_price"])
                else:
                    ws.write(row, 2 + j, "-", self._fmt("na", alt))

            last_col = 2 + len(self.competitors)
            if comp_avgs:
                ws.write(row, last_col, sum(comp_avgs) / len(comp_avgs), self._fmt("euro_eur", alt))
            else:
                ws.write(row, last_col, "-", self._fmt("na", alt))
            row += 1
        row += 2

        # ── Section B: Per verblijfstype ──
        row = self._write_section_header(ws, row, "GEMIDDELDE PRIJS PER VERBLIJFSTYPE")
        row += 1

        by_stay = seasonal.get("by_stay_type", {})

        for c, h in enumerate(month_headers):
            ws.write(row, c, h.replace("Maand", "Verblijfstype"), f["header"])
        row += 1

        stay_order = ["weekend", "midweek", "week", "lang_weekend",
                      "kort_verblijf_2n", "kort_verblijf_4n", "week_overig"]
        for i, stay in enumerate(stay_order):
            if stay not in by_stay:
                continue
            alt = i % 2 == 1
            comps_data = by_stay[stay]
            ws.write(row, 0, STAY_LABELS.get(stay, stay), self._fmt("text", alt))

            wb_data = comps_data.get("Westerbergen")
            if wb_data:
                ws.write(row, 1, wb_data["avg_price"], self._fmt("euro_eur", alt))
            else:
                ws.write(row, 1, "-", self._fmt("na", alt))

            comp_avgs = []
            for j, comp in enumerate(self.competitors):
                cd = comps_data.get(comp)
                if cd:
                    ws.write(row, 2 + j, cd["avg_price"], self._fmt("euro_eur", alt))
                    comp_avgs.append(cd["avg_price"])
                else:
                    ws.write(row, 2 + j, "-", self._fmt("na", alt))

            last_col = 2 + len(self.competitors)
            if comp_avgs:
                ws.write(row, last_col, sum(comp_avgs) / len(comp_avgs), self._fmt("euro_eur", alt))
            else:
                ws.write(row, last_col, "-", self._fmt("na", alt))
            row += 1
        row += 2

        # ── Section C: Concurrentenprofiel ──
        row = self._write_section_header(ws, row, "CONCURRENTENPROFIEL")
        row += 1

        pi_data = self.data.get("price_index", [])
        pos_data = self.data.get("competitive_position", [])

        profile_headers = ["Concurrent", "Gem. Prijs", "Min Prijs", "Max Prijs",
                           "Gem. Index vs WB", "Aantal vergel."]
        for c, h in enumerate(profile_headers):
            ws.write(row, c, h, f["header"])
        row += 1

        by_comp = defaultdict(lambda: {"prices": [], "indices": []})
        for pi in pi_data:
            by_comp[pi["competitor"]]["prices"].append(pi["comp_price"])
            by_comp[pi["competitor"]]["indices"].append(pi["price_index"])

        for i, comp in enumerate(sorted(by_comp.keys())):
            alt = i % 2 == 1
            cd = by_comp[comp]
            ws.write(row, 0, comp, self._fmt("text", alt))
            ws.write(row, 1, sum(cd["prices"]) / len(cd["prices"]), self._fmt("euro_eur", alt))
            ws.write(row, 2, min(cd["prices"]), self._fmt("euro_eur", alt))
            ws.write(row, 3, max(cd["prices"]), self._fmt("euro_eur", alt))
            ws.write(row, 4, sum(cd["indices"]) / len(cd["indices"]), self._fmt("index", alt))
            ws.write(row, 5, len(cd["prices"]), self._fmt("int", alt))
            row += 1

        # Print setup
        ws.set_landscape()
        ws.set_paper(9)
        ws.fit_to_pages(1, 0)

    # ── Sheet 5: Historisch ────────────────────────────────────────────

    def _write_historisch(self):
        ws = self.workbook.add_worksheet("Historisch")
        ws.set_tab_color('#A5A5A5')
        f = self._formats

        ws.merge_range("A1:G1", "HISTORISCH OVERZICHT", f["title"])
        ws.write(1, 0, "Prijswijzigingen tussen scrape-dagen", f["info"])

        changes_data = self.data.get("price_changes", {})
        scrape_dates = changes_data.get("scrape_dates", [])
        ws.write(2, 0, f"Beschikbare scrape-datums: {', '.join(scrape_dates)}", f["info"])

        row = 4

        if changes_data.get("status") == "onvoldoende_data":
            ws.write(row, 0, changes_data.get("message",
                     "Prijswijzigingen worden bijgehouden zodra er meerdere scrape-dagen beschikbaar zijn."),
                     f["info"])
            ws.write(row + 1, 0, f"Momenteel {len(scrape_dates)} scrape-datum(s) beschikbaar.", f["info"])

            ws.set_column(0, 0, 80)
            ws.set_landscape()
            ws.set_paper(9)
            return

        changes = changes_data.get("changes", [])
        row = self._write_section_header(ws, row, "PRIJSWIJZIGINGEN")
        ws.write(row, 0, f"Totaal: {len(changes)} wijzigingen gedetecteerd", f["info"])
        row += 2

        headers = ["Concurrent", "Check-in", "Vorige datum", "Vorige prijs",
                   "Huidige datum", "Huidige prijs", "Verschil EUR", "Verschil %"]
        widths = [20, 12, 12, 13, 13, 13, 13, 11]
        self._set_col_widths(ws, widths)

        for c, h in enumerate(headers):
            ws.write(row, c, h, f["header"])
        header_row = row
        row += 1

        for i, ch in enumerate(changes):
            alt = i % 2 == 1
            ws.write(row, 0, ch.get("competitor_name", ""), self._fmt("text", alt))
            ws.write(row, 1, ch.get("check_in_date", ""), self._fmt("text", alt))
            ws.write(row, 2, ch.get("prev_date", ""), self._fmt("text", alt))
            ws.write(row, 3, ch.get("prev_price", 0), self._fmt("euro_eur", alt))
            ws.write(row, 4, ch.get("curr_date", ""), self._fmt("text", alt))
            ws.write(row, 5, ch.get("curr_price", 0), self._fmt("euro_eur", alt))
            ws.write(row, 6, ch.get("price_change", 0), self._fmt("euro_eur", alt))
            ws.write(row, 7, (ch.get("change_pct", 0) or 0) / 100, self._fmt("pct", alt))
            row += 1

        if changes:
            ws.autofilter(header_row, 0, row - 1, len(headers) - 1)

        row += 2

        # Summary per competitor
        row = self._write_section_header(ws, row, "SAMENVATTING PER CONCURRENT")
        row += 1

        sum_headers = ["Concurrent", "Aantal wijzigingen", "Gem. wijziging EUR", "Gem. wijziging %"]
        for c, h in enumerate(sum_headers):
            ws.write(row, c, h, f["header"])
        row += 1

        by_comp = defaultdict(list)
        for ch in changes:
            by_comp[ch["competitor_name"]].append(ch)

        for i, comp in enumerate(sorted(by_comp.keys())):
            alt = i % 2 == 1
            chs = by_comp[comp]
            avg_change = sum(c.get("price_change", 0) for c in chs) / len(chs)
            avg_pct = sum(c.get("change_pct", 0) for c in chs) / len(chs)
            ws.write(row, 0, comp, self._fmt("text", alt))
            ws.write(row, 1, len(chs), self._fmt("int", alt))
            ws.write(row, 2, avg_change, self._fmt("euro_eur", alt))
            ws.write(row, 3, avg_pct / 100, self._fmt("pct", alt))
            row += 1

        ws.set_landscape()
        ws.set_paper(9)
        ws.fit_to_pages(1, 0)

"""KPI computation engine: pure functions that take normalized comparison data."""

from collections import defaultdict


def compute_price_index(comparison_data: list[dict]) -> list[dict]:
    """Compute Westerbergen price / competitor price * 100.

    Index < 100 = WB is cheaper, > 100 = WB is more expensive.
    """
    results = []
    for row in comparison_data:
        if row["wb_price"] is None:
            continue
        for comp_name, comp_data in row["competitors"].items():
            if not comp_data["price"] or not comp_data["available"]:
                continue
            index = round(row["wb_price"] / comp_data["price"] * 100, 1)
            verschil = round(row["wb_price"] - comp_data["price"], 2)
            verschil_pct = round(verschil / comp_data["price"] * 100, 1)
            results.append({
                "check_in_date": row["check_in_date"],
                "nights": row["nights"],
                "stay_type": row["stay_type"],
                "competitor": comp_name,
                "wb_price": row["wb_price"],
                "comp_price": comp_data["price"],
                "price_index": index,
                "verschil_eur": verschil,
                "verschil_pct": verschil_pct,
            })
    return results


def compute_price_per_night(comparison_data: list[dict]) -> list[dict]:
    """Compute price/night for WB and each competitor with ranking."""
    results = []
    for row in comparison_data:
        if row["wb_price"] is None:
            continue
        wb_ppn = row["wb_price_per_night"]
        comp_ppns = {}
        all_ppns = [("Westerbergen", wb_ppn)]
        for comp_name, comp_data in row["competitors"].items():
            if comp_data["price"] and comp_data["available"]:
                ppn = comp_data["price_per_night"]
                comp_ppns[comp_name] = ppn
                all_ppns.append((comp_name, ppn))
        if len(all_ppns) < 2:
            continue
        all_ppns.sort(key=lambda x: x[1])
        rank = next(i + 1 for i, (name, _) in enumerate(all_ppns) if name == "Westerbergen")
        results.append({
            "check_in_date": row["check_in_date"],
            "nights": row["nights"],
            "stay_type": row["stay_type"],
            "wb_ppn": wb_ppn,
            "competitors": comp_ppns,
            "wb_rank_ppn": rank,
            "total_competitors": len(all_ppns),
        })
    return results


def compute_competitive_position(comparison_data: list[dict]) -> list[dict]:
    """Rank WB among all competitors by total price per (date, nights)."""
    results = []
    for row in comparison_data:
        if row["wb_price"] is None:
            continue
        all_prices = [("Westerbergen", row["wb_price"])]
        for comp_name, comp_data in row["competitors"].items():
            if comp_data["price"] and comp_data["available"]:
                all_prices.append((comp_name, comp_data["price"]))
        if len(all_prices) < 2:
            continue
        all_prices.sort(key=lambda x: x[1])
        rank = next(i + 1 for i, (name, _) in enumerate(all_prices) if name == "Westerbergen")
        results.append({
            "check_in_date": row["check_in_date"],
            "nights": row["nights"],
            "stay_type": row["stay_type"],
            "wb_rank": rank,
            "total_parks": len(all_prices),
            "ranking": all_prices,
        })
    return results


def compute_availability_gaps(comparison_data: list[dict]) -> dict:
    """Identify dates where availability differs between WB and competitors."""
    wb_open_comp_closed = []  # Opportunity to raise prices
    wb_closed_comp_open = []  # Missed revenue

    for row in comparison_data:
        for comp_name, comp_data in row["competitors"].items():
            if row["wb_available"] and not comp_data["available"]:
                wb_open_comp_closed.append({
                    "check_in_date": row["check_in_date"],
                    "nights": row["nights"],
                    "stay_type": row["stay_type"],
                    "wb_price": row["wb_price"],
                    "competitor": comp_name,
                })
            elif not row["wb_available"] and comp_data["available"] and comp_data["price"]:
                wb_closed_comp_open.append({
                    "check_in_date": row["check_in_date"],
                    "nights": row["nights"],
                    "stay_type": row["stay_type"],
                    "competitor": comp_name,
                    "comp_price": comp_data["price"],
                })

    # Summarize per competitor
    raise_by_comp = defaultdict(int)
    for item in wb_open_comp_closed:
        raise_by_comp[item["competitor"]] += 1

    missed_by_comp = defaultdict(int)
    for item in wb_closed_comp_open:
        missed_by_comp[item["competitor"]] += 1

    return {
        "wb_open_comp_closed": wb_open_comp_closed,
        "wb_closed_comp_open": wb_closed_comp_open,
        "raise_by_competitor": dict(raise_by_comp),
        "missed_by_competitor": dict(missed_by_comp),
        "summary": {
            "raise_opportunity_count": len(wb_open_comp_closed),
            "missed_revenue_count": len(wb_closed_comp_open),
        },
    }


def compute_seasonal_patterns(comparison_data: list[dict]) -> dict:
    """Average prices per month, per stay_type, per competitor."""
    # Accumulators: {group_key: {competitor: [prices]}}
    by_month = defaultdict(lambda: defaultdict(list))
    by_stay = defaultdict(lambda: defaultdict(list))
    by_month_stay = defaultdict(lambda: defaultdict(list))

    for row in comparison_data:
        month = row["month"]
        stay = row["stay_type"]

        if row["wb_price"] is not None:
            by_month[month]["Westerbergen"].append(row["wb_price"])
            by_stay[stay]["Westerbergen"].append(row["wb_price"])
            by_month_stay[(month, stay)]["Westerbergen"].append(row["wb_price"])

        for comp_name, comp_data in row["competitors"].items():
            if comp_data["price"] and comp_data["available"]:
                by_month[month][comp_name].append(comp_data["price"])
                by_stay[stay][comp_name].append(comp_data["price"])
                by_month_stay[(month, stay)][comp_name].append(comp_data["price"])

    def _summarize(accumulator):
        result = {}
        for key, competitors in sorted(accumulator.items()):
            result[key] = {}
            for comp, prices in sorted(competitors.items()):
                result[key][comp] = {
                    "avg_price": round(sum(prices) / len(prices), 2),
                    "min_price": min(prices),
                    "max_price": max(prices),
                    "count": len(prices),
                }
        return result

    return {
        "by_month": _summarize(by_month),
        "by_stay_type": _summarize(by_stay),
        "by_month_and_stay": _summarize(by_month_stay),
    }


def compute_price_changes(db) -> dict:
    """Track price changes across scrape days.

    Returns empty result if only 1 scrape day exists.
    """
    conn = db._get_conn()
    try:
        dates = conn.execute(
            "SELECT DISTINCT scrape_date FROM prices ORDER BY scrape_date"
        ).fetchall()
        scrape_dates = [r[0] for r in dates]
    finally:
        conn.close()

    if len(scrape_dates) < 2:
        return {
            "status": "onvoldoende_data",
            "message": "Prijswijzigingen worden bijgehouden zodra er meerdere scrape-dagen beschikbaar zijn.",
            "scrape_dates": scrape_dates,
            "changes": [],
        }

    # Find changes between consecutive scrape days
    conn = db._get_conn()
    try:
        rows = conn.execute("""
            SELECT
                p2.competitor_name,
                p2.check_in_date,
                p2.check_out_date,
                p1.scrape_date AS prev_date,
                p2.scrape_date AS curr_date,
                p1.price AS prev_price,
                p2.price AS curr_price,
                ROUND(p2.price - p1.price, 2) AS price_change,
                ROUND((p2.price - p1.price) / p1.price * 100, 1) AS change_pct
            FROM prices p1
            JOIN prices p2 ON p1.competitor_name = p2.competitor_name
                AND p1.check_in_date = p2.check_in_date
                AND p1.check_out_date = p2.check_out_date
            WHERE p2.scrape_date > p1.scrape_date
              AND p1.price IS NOT NULL
              AND p2.price IS NOT NULL
              AND p1.price != p2.price
              AND p1.price > 0
              AND p2.scrape_date = (
                  SELECT MIN(scrape_date) FROM prices
                  WHERE scrape_date > p1.scrape_date
                    AND competitor_name = p1.competitor_name
                    AND check_in_date = p1.check_in_date
                    AND check_out_date = p1.check_out_date
              )
            ORDER BY p2.scrape_date DESC, p1.competitor_name, p1.check_in_date
        """).fetchall()
        changes = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "status": "ok",
        "scrape_dates": scrape_dates,
        "total_changes": len(changes),
        "changes": changes,
    }


def compute_recommendations(
    price_index_data: list[dict],
    position_data: list[dict],
    availability_gaps: dict,
    seasonal_data: dict,
) -> list[dict]:
    """Generate actionable pricing recommendations."""
    recs = {}  # keyed by (check_in_date, nights) to avoid duplicates

    # Rule 1: WB is much cheaper than competitors (price_index < 70)
    for pi in price_index_data:
        key = (pi["check_in_date"], pi["nights"])
        if pi["price_index"] < 70:
            if key not in recs:
                recs[key] = {
                    "check_in_date": pi["check_in_date"],
                    "nights": pi["nights"],
                    "stay_type": pi["stay_type"],
                    "type": "verhoging",
                    "urgentie": "hoog" if pi["price_index"] < 50 else "middel",
                    "huidig_prijs": pi["wb_price"],
                    "concurrenten": {},
                    "redenen": [],
                }
            rec = recs[key]
            rec["concurrenten"][pi["competitor"]] = pi["comp_price"]
            reden = (
                f"WB is {abs(pi['verschil_pct']):.0f}% goedkoper dan "
                f"{pi['competitor']} (EUR {pi['comp_price']:.0f})"
            )
            if reden not in rec["redenen"]:
                rec["redenen"].append(reden)

    # Rule 2: Competitors sold out, WB available
    sold_out_dates = defaultdict(list)
    for item in availability_gaps.get("wb_open_comp_closed", []):
        key = (item["check_in_date"], item["nights"])
        sold_out_dates[key].append(item["competitor"])

    for key, comps in sold_out_dates.items():
        if len(comps) >= 2:  # At least 2 competitors sold out
            if key not in recs:
                # Find wb_price from position_data
                wb_price = None
                stay_type = None
                for pos in position_data:
                    if (pos["check_in_date"], pos["nights"]) == key:
                        wb_price = next(p for n, p in pos["ranking"] if n == "Westerbergen")
                        stay_type = pos["stay_type"]
                        break
                if wb_price is None:
                    continue
                recs[key] = {
                    "check_in_date": key[0],
                    "nights": key[1],
                    "stay_type": stay_type,
                    "type": "verhoging",
                    "urgentie": "hoog",
                    "huidig_prijs": wb_price,
                    "concurrenten": {},
                    "redenen": [],
                }
            rec = recs[key]
            rec["redenen"].append(
                f"Hoge vraag: {', '.join(comps)} uitverkocht"
            )

    # Rule 3: WB is most expensive (last rank) and >20% above average
    for pos in position_data:
        key = (pos["check_in_date"], pos["nights"])
        if pos["wb_rank"] == pos["total_parks"]:
            wb_price = next(p for n, p in pos["ranking"] if n == "Westerbergen")
            comp_prices = [p for n, p in pos["ranking"] if n != "Westerbergen"]
            if comp_prices:
                avg_comp = sum(comp_prices) / len(comp_prices)
                if wb_price > avg_comp * 1.20 and key not in recs:
                    recs[key] = {
                        "check_in_date": pos["check_in_date"],
                        "nights": pos["nights"],
                        "stay_type": pos["stay_type"],
                        "type": "verlaging",
                        "urgentie": "middel",
                        "huidig_prijs": wb_price,
                        "concurrenten": {n: p for n, p in pos["ranking"] if n != "Westerbergen"},
                        "redenen": [f"WB is duurste aanbieder, {((wb_price/avg_comp)-1)*100:.0f}% boven gemiddelde"],
                    }

    # Calculate suggested prices
    for rec in recs.values():
        if rec["type"] == "verhoging" and rec["concurrenten"]:
            cheapest_comp = min(rec["concurrenten"].values())
            suggested = round(cheapest_comp * 0.85, 0)
            rec["voorgesteld_prijs"] = max(suggested, rec["huidig_prijs"])
            rec["extra_omzet"] = rec["voorgesteld_prijs"] - rec["huidig_prijs"]
        elif rec["type"] == "verlaging" and rec["concurrenten"]:
            avg_comp = sum(rec["concurrenten"].values()) / len(rec["concurrenten"])
            rec["voorgesteld_prijs"] = round(avg_comp * 0.90, 0)
            rec["extra_omzet"] = rec["voorgesteld_prijs"] - rec["huidig_prijs"]
        else:
            rec["voorgesteld_prijs"] = None
            rec["extra_omzet"] = 0

        # Combine reasons into single string
        rec["reden"] = "; ".join(rec["redenen"])
        del rec["redenen"]

    # Sort by urgency (hoog first) then by extra_omzet descending
    urgency_order = {"hoog": 0, "middel": 1, "laag": 2}
    result = sorted(
        recs.values(),
        key=lambda r: (urgency_order.get(r["urgentie"], 9), -(r.get("extra_omzet") or 0)),
    )

    return result

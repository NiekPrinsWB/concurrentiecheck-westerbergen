"""Dutch-language console report formatter for analytics results."""

import sys
from collections import defaultdict


def _header(title: str) -> str:
    return f"\n--- {title} ---"


def _table_row(cols: list[str], widths: list[int]) -> str:
    parts = []
    for col, w in zip(cols, widths):
        parts.append(str(col).ljust(w) if w > 0 else str(col).rjust(-w))
    return "  " + " | ".join(parts)


def _separator(widths: list[int]) -> str:
    return "  " + "-+-".join("-" * abs(w) for w in widths)


def print_report(result: dict):
    """Print the full Dutch-language analytics report to console."""
    # Ensure UTF-8 output on Windows
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    meta = result["metadata"]
    print("=" * 72)
    print("  CONCURRENTIECHECK WESTERBERGEN - ANALYTICS RAPPORT")
    print(f"  Scrape datum: {meta['scrape_date']}")
    print(f"  Vergelijkbare combinaties: {meta['comparison_count']} (datum+verblijfsduur)")
    print(f"  Concurrenten: {', '.join(sorted(meta['competitors']))}")
    print("=" * 72)

    _print_price_index_summary(result["price_index"])
    _print_price_per_night(result["price_per_night"])
    _print_competitive_position(result["competitive_position"])
    _print_availability_gaps(result["availability_gaps"])
    _print_seasonal_patterns(result["seasonal_patterns"])
    _print_price_changes(result["price_changes"])
    _print_recommendations(result["recommendations"])

    print("\n" + "=" * 72)


def _print_price_index_summary(price_index_data: list[dict]):
    print(_header("1. PRIJSINDEX SAMENVATTING"))
    print("  (index < 100 = WB goedkoper, > 100 = WB duurder)\n")

    if not price_index_data:
        print("  Geen vergelijkbare data beschikbaar.")
        return

    # Aggregate per competitor
    by_comp = defaultdict(list)
    for pi in price_index_data:
        by_comp[pi["competitor"]].append(pi["price_index"])

    widths = [24, -10, -10, -10, -12]
    print(_table_row(["Concurrent", "Gem.", "Min", "Max", "Vergelijk."], widths))
    print(_separator(widths))

    for comp in sorted(by_comp.keys()):
        indices = by_comp[comp]
        avg = sum(indices) / len(indices)
        print(_table_row([
            comp,
            f"{avg:.1f}",
            f"{min(indices):.1f}",
            f"{max(indices):.1f}",
            str(len(indices)),
        ], widths))

    # Overall
    all_indices = [pi["price_index"] for pi in price_index_data]
    avg_all = sum(all_indices) / len(all_indices)
    print()
    if avg_all < 100:
        print(f"  Westerbergen is gemiddeld {100 - avg_all:.0f}% goedkoper dan de concurrentie.")
    else:
        print(f"  Westerbergen is gemiddeld {avg_all - 100:.0f}% duurder dan de concurrentie.")


def _print_price_per_night(ppn_data: list[dict]):
    print(_header("2. PRIJS PER NACHT"))
    print("  Genormaliseerde vergelijking per verblijfsduur\n")

    if not ppn_data:
        print("  Geen data beschikbaar.")
        return

    stay_labels = {
        "weekend": "Weekend (2n)",
        "midweek": "Midweek (4n)",
        "week": "Week (7n)",
        "lang_weekend": "Lang weekend (3n)",
        "kort_verblijf_2n": "Kort verblijf (2n)",
        "kort_verblijf_4n": "Kort verblijf (4n)",
        "week_overig": "Week overig (7n)",
    }

    # Group by stay_type
    by_stay = defaultdict(list)
    for row in ppn_data:
        by_stay[row["stay_type"]].append(row)

    widths = [20, -12, -22, -22]
    print(_table_row(["Verblijfsduur", "WB Gem. p/n", "Goedkoopste conc.", "Duurste conc."], widths))
    print(_separator(widths))

    for stay in ["weekend", "midweek", "week", "lang_weekend", "kort_verblijf_2n",
                  "kort_verblijf_4n", "week_overig"]:
        rows = by_stay.get(stay, [])
        if not rows:
            continue

        wb_ppns = [r["wb_ppn"] for r in rows]
        avg_wb = sum(wb_ppns) / len(wb_ppns)

        # Find cheapest and most expensive competitor across all rows
        all_comp_ppns = defaultdict(list)
        for r in rows:
            for comp, ppn in r["competitors"].items():
                all_comp_ppns[comp].append(ppn)

        if all_comp_ppns:
            comp_avgs = {c: sum(ps) / len(ps) for c, ps in all_comp_ppns.items()}
            cheapest = min(comp_avgs.items(), key=lambda x: x[1])
            duurste = max(comp_avgs.items(), key=lambda x: x[1])
        else:
            cheapest = ("-", 0)
            duurste = ("-", 0)

        label = stay_labels.get(stay, stay)
        print(_table_row([
            label,
            f"EUR {avg_wb:>6.0f}",
            f"{cheapest[0]} EUR {cheapest[1]:.0f}",
            f"{duurste[0]} EUR {duurste[1]:.0f}",
        ], widths))


def _print_competitive_position(position_data: list[dict]):
    print(_header("3. CONCURRENTIEPOSITIE"))
    print("  Ranking per datum+verblijfsduur (1 = goedkoopst)\n")

    if not position_data:
        print("  Geen data beschikbaar.")
        return

    rank_counts = defaultdict(int)
    for pos in position_data:
        if pos["wb_rank"] <= 2:
            rank_counts[pos["wb_rank"]] += 1
        else:
            rank_counts["3+"] += 1

    total = len(position_data)
    widths = [10, -14, -12]
    print(_table_row(["Positie", "Aantal keren", "Percentage"], widths))
    print(_separator(widths))

    for rank_label in [1, 2, "3+"]:
        count = rank_counts.get(rank_label, 0)
        pct = count / total * 100 if total else 0
        print(_table_row([
            f"#{rank_label}",
            str(count),
            f"{pct:.1f}%",
        ], widths))

    # Summary
    pct_cheapest = rank_counts.get(1, 0) / total * 100 if total else 0
    print()
    print(f"  Westerbergen is in {pct_cheapest:.0f}% van de gevallen de goedkoopste aanbieder.")


def _print_availability_gaps(gaps: dict):
    print(_header("4. BESCHIKBAARHEID ANALYSE"))

    raise_comps = gaps.get("raise_by_competitor", {})
    missed_comps = gaps.get("missed_by_competitor", {})

    print("\n  Concurrenten uitverkocht, WB beschikbaar (= kans voor prijsverhoging):")
    if raise_comps:
        for comp, count in sorted(raise_comps.items(), key=lambda x: -x[1]):
            print(f"    {comp}: {count} combinaties")
    else:
        print("    Geen (alle concurrenten beschikbaar)")

    print("\n  WB uitverkocht, concurrenten beschikbaar (= gemiste omzet):")
    if missed_comps:
        for comp, count in sorted(missed_comps.items(), key=lambda x: -x[1]):
            print(f"    {comp}: {count} combinaties")
    else:
        print("    Geen (alle datums beschikbaar)")


def _print_seasonal_patterns(seasonal: dict):
    print(_header("5. SEIZOENSPATRONEN"))
    print("  Gemiddelde prijs per maand (Westerbergen vs. gemiddelde concurrent)\n")

    by_month = seasonal.get("by_month", {})
    if not by_month:
        print("  Geen data beschikbaar.")
        return

    widths = [12, -10, -12, -8]
    print(_table_row(["Maand", "WB Gem.", "Conc. Gem.", "Index"], widths))
    print(_separator(widths))

    for month in sorted(by_month.keys()):
        comps = by_month[month]
        wb_data = comps.get("Westerbergen")
        if wb_data is None:
            continue

        # Average of all competitor averages
        comp_avgs = [
            data["avg_price"] for name, data in comps.items()
            if name != "Westerbergen"
        ]
        if comp_avgs:
            avg_comp = sum(comp_avgs) / len(comp_avgs)
            index = round(wb_data["avg_price"] / avg_comp * 100, 1)
        else:
            avg_comp = 0
            index = "-"

        print(_table_row([
            month,
            f"EUR {wb_data['avg_price']:>6.0f}",
            f"EUR {avg_comp:>6.0f}" if avg_comp else "-",
            str(index),
        ], widths))


def _print_price_changes(changes: dict):
    print(_header("6. PRIJSWIJZIGINGEN"))

    if changes.get("status") == "onvoldoende_data":
        print(f"  {changes['message']}")
        return

    total = changes.get("total_changes", 0)
    print(f"\n  Totaal {total} prijswijzigingen gedetecteerd.\n")

    if total == 0:
        print("  Geen prijswijzigingen sinds vorige scrape.")
        return

    # Show top 10 biggest changes
    sorted_changes = sorted(
        changes["changes"],
        key=lambda c: abs(c.get("change_pct", 0)),
        reverse=True,
    )[:10]

    widths = [20, 12, -10, -10, -10]
    print(_table_row(["Concurrent", "Check-in", "Was", "Nu", "Wijziging"], widths))
    print(_separator(widths))

    for c in sorted_changes:
        print(_table_row([
            c["competitor_name"],
            c["check_in_date"],
            f"EUR {c['prev_price']:.0f}",
            f"EUR {c['curr_price']:.0f}",
            f"{c['change_pct']:+.1f}%",
        ], widths))


def _print_recommendations(recommendations: list[dict]):
    print(_header("7. PRIJSADVIEZEN"))

    if not recommendations:
        print("  Geen specifieke adviezen op basis van huidige data.")
        return

    # Show top 15
    top = recommendations[:15]

    print(f"\n  Top {len(top)} adviezen (van {len(recommendations)} totaal):\n")

    widths = [12, -5, -10, -12, -12, 40]
    print(_table_row(["Datum", "Duur", "Huidig", "Advies", "Voorgesteld", "Reden"], widths))
    print(_separator(widths))

    for rec in top:
        advies = rec["type"].upper()
        urgentie = rec["urgentie"]
        if urgentie == "hoog":
            advies += " !"
        voorgesteld = f"EUR {rec['voorgesteld_prijs']:.0f}" if rec.get("voorgesteld_prijs") else "-"

        # Truncate reason to fit
        reden = rec.get("reden", "")
        if len(reden) > 55:
            reden = reden[:52] + "..."

        print(_table_row([
            rec["check_in_date"],
            f"{rec['nights']}n",
            f"EUR {rec['huidig_prijs']:.0f}",
            advies,
            voorgesteld,
            reden,
        ], widths))

    # Summary
    verhogingen = [r for r in recommendations if r["type"] == "verhoging"]
    verlagingen = [r for r in recommendations if r["type"] == "verlaging"]
    total_extra = sum(r.get("extra_omzet", 0) for r in verhogingen if r.get("extra_omzet", 0) > 0)

    print(f"\n  Totaal: {len(verhogingen)} verhogingen, {len(verlagingen)} verlagingen")
    if total_extra > 0:
        print(f"  Geschatte extra omzet bij verhogingen: EUR {total_extra:,.0f}")

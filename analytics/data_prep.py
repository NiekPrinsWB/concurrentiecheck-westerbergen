"""Data preparation: load raw prices and build normalized comparison sets."""

from collections import defaultdict
from datetime import datetime

OWN_PARK = "Westerbergen"
CANONICAL_DURATIONS = [2, 3, 4, 7]

DOW_NAMES = {
    0: "maandag", 1: "dinsdag", 2: "woensdag", 3: "donderdag",
    4: "vrijdag", 5: "zaterdag", 6: "zondag",
}


def classify_stay(check_in_date: str, nights: int) -> str:
    """Classify a stay based on check-in day and duration."""
    dt = datetime.strptime(check_in_date, "%Y-%m-%d")
    dow = dt.weekday()  # 0=Monday ... 6=Sunday

    if nights == 2 and dow == 4:
        return "weekend"
    elif nights == 4 and dow == 0:
        return "midweek"
    elif nights == 7 and dow == 4:
        return "week"
    elif nights == 3:
        return "lang_weekend"
    elif nights == 2:
        return "kort_verblijf_2n"
    elif nights == 4:
        return "kort_verblijf_4n"
    elif nights == 7:
        return "week_overig"
    else:
        return f"{nights}_nachten"


def load_comparison_data(db, scrape_date: str = None) -> list[dict]:
    """Load and normalize all price data into comparison rows.

    Each row represents one (check_in_date, nights) combination with
    Westerbergen's price and all available competitor prices.

    Only includes rows where Westerbergen has a price.
    """
    if scrape_date is None:
        scrape_date = db.get_latest_scrape_date()
        if scrape_date is None:
            return []

    raw = db.get_comparison_data(scrape_date, CANONICAL_DURATIONS)
    today = datetime.now().date()

    # Group by (check_in_date, nights)
    groups = defaultdict(list)
    for row in raw:
        key = (row["check_in_date"], row["nights"])
        groups[key].append(row)

    comparison_rows = []
    for (check_in, nights), rows in sorted(groups.items()):
        # Find Westerbergen in this group
        wb_row = None
        comp_rows = []
        for r in rows:
            if r["competitor_name"] == OWN_PARK:
                wb_row = r
            else:
                comp_rows.append(r)

        # Skip if no Westerbergen price
        if wb_row is None or wb_row["price"] is None:
            continue

        # Skip if no competitors at all
        if not comp_rows:
            continue

        check_in_dt = datetime.strptime(check_in, "%Y-%m-%d").date()
        days_ahead = (check_in_dt - today).days

        competitors = {}
        for cr in comp_rows:
            competitors[cr["competitor_name"]] = {
                "price": cr["price"],
                "available": bool(cr["available"]),
                "price_per_night": round(cr["price"] / nights, 2) if cr["price"] else None,
            }

        comparison_rows.append({
            "check_in_date": check_in,
            "check_out_date": wb_row["check_out_date"],
            "nights": nights,
            "stay_type": classify_stay(check_in, nights),
            "month": check_in[:7],
            "day_of_week": DOW_NAMES[check_in_dt.weekday()],
            "days_ahead": days_ahead,
            "wb_price": wb_row["price"],
            "wb_available": bool(wb_row["available"]),
            "wb_price_per_night": round(wb_row["price"] / nights, 2),
            "competitors": competitors,
        })

    return comparison_rows

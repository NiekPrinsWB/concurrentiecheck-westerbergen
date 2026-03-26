# Concurrentiecheck Westerbergen

## Project overview
Geautomatiseerde prijsvergelijking van Westerbergen (vakantieparken) met 6 concurrenten.
Scraped prijzen, slaat op in SQLite, berekent analytics, genereert Excel dashboard, en pusht naar GitHub voor Streamlit Cloud.

## Projectlocatie
- **Werkdirectory**: `C:\Users\Niek\Desktop\Concurrentiecheck WB\`
- **GitHub repo**: `NiekPrinsWB/concurrentiecheck-westerbergen`
- **Streamlit app**: wordt automatisch bijgewerkt via GitHub push
- **Scheduled task**: "Concurrentiecheck Westerbergen", elke vrijdag 10:00

## "Draai een run" / "Start scraper" workflow
Wanneer de gebruiker zegt "draai een run", "start scraper", "volledige run", of iets vergelijkbaars:

1. **Ga naar de juiste directory**: `C:\Users\Niek\Desktop\Concurrentiecheck WB\`
2. **Draai de pipeline**: `python run_daily.py`
   - Dit doet automatisch ALLES: alle 7 scrapers, analytics, Excel dashboard, git push, en email
   - Duurt ~45 minuten (12 maanden data, ~6000 records)
   - Draai als background task met timeout van 600000ms
3. **Check het logbestand** in `logs/scraper_YYYYMMDD_HHMMSS.log` voor voortgang
4. **Verwachte output**:
   - 7/7 scrapers OK (beerze_bulten, camping_ommerland, eiland_van_maurik, witter_zomer, de_witte_berg, de_boshoek, westerbergen)
   - Analytics met vergelijkingen en prijsadviezen
   - Excel dashboard in `data/concurrentiecheck_YYYY-MM-DD.xlsx`
   - Database gepusht naar GitHub (Streamlit Cloud auto-update)
   - Email verstuurd naar nprins@westerbergen.nl, jrgklein@westerbergen.nl, jvenema@westerbergen.nl
5. **Rapporteer samenvatting** aan gebruiker: scrapers status, records, analytics highlights, email status, git push status

## CLI opties
```
python run_daily.py                  # Volledige run
python run_daily.py --dry-run        # Test zonder scraping
python run_daily.py --skip-scrape    # Alleen analytics + dashboard + email
```

## Architectuur
- `run_daily.py` - Hoofdpipeline (scrape -> analytics -> dashboard -> git push -> email)
- `run_scraper.py` - Scraper orchestration
- `scrapers/` - Per-concurrent scrapers (Playwright + API-based)
- `analytics/` - Prijsanalyse, KPI's, aanbevelingen
- `dashboard/` - Excel generator
- `database.py` - SQLite ORM
- `email_report.py` - Email notificaties via Gmail SMTP
- `config/settings.yaml` - Configuratie
- `.env` - SMTP credentials (niet in git)
- `streamlit_app/` - Streamlit Cloud dashboard

## Belangrijke noten
- **Draai ALTIJD vanuit `C:\Users\Niek\Desktop\Concurrentiecheck WB\`**, NIET vanuit `WB-compcheck` of een GitHub clone
- De `.env` bevat Gmail SMTP credentials voor `nmphospitality85@gmail.com`
- Bij git push conflicten: de lokale database is altijd de meest complete, gebruik `git checkout --ours`
- Python 3.14 op dit systeem, dependencies staan in `requirements-dev.txt`

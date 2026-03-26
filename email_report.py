"""E-mail rapport versturen na afloop van de wekelijkse scrape.

Verstuurt een samenvatting met het Excel dashboard als bijlage
via Microsoft 365 SMTP.
"""

import logging
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

logger = logging.getLogger(__name__)


def _load_env():
    """Load .env file from project root into os.environ."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def send_report(
    scrape_results: dict,
    excel_path: str | None,
    total_duration: float,
) -> bool:
    """Send the weekly scrape report via email.

    Args:
        scrape_results: Dict of {scraper_key: {status, records, available, duration, ...}}
        excel_path: Path to the Excel dashboard file (attached if present).
        total_duration: Total pipeline duration in seconds.

    Returns:
        True if email was sent successfully, False otherwise.
    """
    _load_env()

    smtp_server = os.environ.get("SMTP_SERVER", "smtp.office365.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_password = os.environ.get("SMTP_PASSWORD", "")

    if not smtp_user or not smtp_password or smtp_password == "VULMETHIERHETAPPPASSWORD":
        logger.warning("SMTP niet geconfigureerd - e-mail overgeslagen")
        logger.warning("Vul SMTP_USER en SMTP_PASSWORD in in .env")
        return False

    to_addr = "nprins@westerbergen.nl"
    cc_addr = "jrgklein@westerbergen.nl"
    cc_addr2 = "jvenema@westerbergen.nl"

    # Build email
    msg = MIMEMultipart()
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg["Cc"] = f"{cc_addr}, {cc_addr2}"
    msg["Subject"] = _build_subject(scrape_results)

    body = _build_body(scrape_results, excel_path, total_duration)
    msg.attach(MIMEText(body, "html", "utf-8"))

    # Attach Excel dashboard
    if excel_path and os.path.isfile(excel_path):
        try:
            with open(excel_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            filename = os.path.basename(excel_path)
            part.add_header("Content-Disposition", f"attachment; filename={filename}")
            msg.attach(part)
            logger.info(f"  Excel bijlage: {filename}")
        except Exception as e:
            logger.error(f"  Bijlage toevoegen mislukt: {e}")

    # Send
    all_recipients = [to_addr, cc_addr, cc_addr2]
    try:
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, all_recipients, msg.as_string())
        logger.info(f"  E-mail verstuurd naar {to_addr} (cc: {cc_addr}, {cc_addr2})")
        return True
    except Exception as e:
        logger.error(f"  E-mail versturen mislukt: {e}")
        return False


def _build_subject(scrape_results: dict) -> str:
    """Build email subject line."""
    from datetime import datetime

    today = datetime.now().strftime("%d-%m-%Y")
    success = sum(1 for r in scrape_results.values() if "success" in r.get("status", ""))
    total = len(scrape_results)
    failed = total - success

    if failed == 0:
        return f"Concurrentiecheck {today} - Voltooid ({success} scrapers OK)"
    else:
        return f"Concurrentiecheck {today} - {failed} van {total} scrapers mislukt"


def _build_body(
    scrape_results: dict,
    excel_path: str | None,
    total_duration: float,
) -> str:
    """Build HTML email body with scrape summary."""
    from datetime import datetime

    today = datetime.now().strftime("%d-%m-%Y %H:%M")

    total_records = sum(r.get("records", 0) for r in scrape_results.values())
    total_available = sum(r.get("available", 0) for r in scrape_results.values())

    # Build scraper rows
    rows = ""
    for key, result in sorted(scrape_results.items()):
        status = result.get("status", "?")
        records = result.get("records", 0)
        available = result.get("available", 0)
        dur = result.get("duration", 0)
        error = result.get("error", "")

        if "success" in status:
            color = "#28a745"
            icon = "&#10004;"
        else:
            color = "#dc3545"
            icon = "&#10008;"

        name = key.replace("_", " ").title()
        error_cell = f'<td style="color:#dc3545;font-size:12px">{error[:80]}</td>' if error else '<td></td>'

        rows += f"""
        <tr>
            <td><span style="color:{color}">{icon}</span> {name}</td>
            <td style="text-align:right">{records}</td>
            <td style="text-align:right">{available}</td>
            <td style="text-align:right">{dur:.0f}s</td>
            {error_cell}
        </tr>"""

    attachment_note = ""
    if excel_path and os.path.isfile(excel_path):
        filename = os.path.basename(excel_path)
        attachment_note = f'<p>Het Excel dashboard (<strong>{filename}</strong>) is bijgevoegd.</p>'
    else:
        attachment_note = '<p style="color:#dc3545">Geen Excel dashboard beschikbaar.</p>'

    return f"""
    <html>
    <body style="font-family: Calibri, Arial, sans-serif; color: #333; max-width: 700px;">
        <h2 style="color: #2c5f2d;">Concurrentiecheck Westerbergen</h2>
        <p>Wekelijkse prijsscraping voltooid op <strong>{today}</strong></p>
        <p>Totale duur: <strong>{total_duration:.0f} seconden</strong> ({total_duration/60:.1f} min)</p>

        <table style="border-collapse:collapse; width:100%; margin:15px 0;" cellpadding="6">
            <thead>
                <tr style="background:#2c5f2d; color:white;">
                    <th style="text-align:left">Scraper</th>
                    <th style="text-align:right">Records</th>
                    <th style="text-align:right">Beschikbaar</th>
                    <th style="text-align:right">Duur</th>
                    <th>Fout</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
            <tfoot>
                <tr style="border-top:2px solid #2c5f2d; font-weight:bold;">
                    <td>Totaal</td>
                    <td style="text-align:right">{total_records}</td>
                    <td style="text-align:right">{total_available}</td>
                    <td style="text-align:right">{total_duration:.0f}s</td>
                    <td></td>
                </tr>
            </tfoot>
        </table>

        {attachment_note}

        <p style="color:#888; font-size:12px; margin-top:30px;">
            Dit bericht is automatisch verstuurd door de Concurrentiecheck Westerbergen pipeline.
        </p>
    </body>
    </html>
    """

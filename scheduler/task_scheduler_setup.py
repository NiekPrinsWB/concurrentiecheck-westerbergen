"""Windows Task Scheduler registratie voor dagelijkse pipeline.

Registreert/verwijdert een dagelijkse taak in Windows Task Scheduler
via het schtasks.exe commando.

Usage:
    python scheduler/task_scheduler_setup.py --install          # Registreer taak
    python scheduler/task_scheduler_setup.py --install --time 08:00
    python scheduler/task_scheduler_setup.py --uninstall        # Verwijder taak
    python scheduler/task_scheduler_setup.py --status           # Toon status

Vereist: uitvoeren als administrator voor --install/--uninstall.
"""

import argparse
import os
import subprocess
import sys

# Taak configuratie
TASK_NAME = "ConcurrentiecheckWesterbergen"
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BAT_FILE = os.path.join(PROJECT_DIR, "run_daily.bat")


def check_admin():
    """Controleer of het script als administrator draait."""
    try:
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


def install_task(schedule_time: str = "07:00"):
    """Registreer dagelijkse taak in Windows Task Scheduler."""
    if not check_admin():
        print("WAARSCHUWING: Dit script moet als administrator worden uitgevoerd.")
        print("Klik rechts op Command Prompt -> 'Als administrator uitvoeren'")
        print()

    if not os.path.exists(BAT_FILE):
        print(f"FOUT: {BAT_FILE} niet gevonden.")
        print("Zorg dat run_daily.bat in de projectmap staat.")
        sys.exit(1)

    # schtasks /create commando
    cmd = [
        "schtasks", "/create",
        "/tn", TASK_NAME,
        "/tr", f'"{BAT_FILE}"',
        "/sc", "DAILY",
        "/st", schedule_time,
        "/f",  # Force: overschrijf bestaande taak
        "/rl", "HIGHEST",  # Hoogste privileges
    ]

    print(f"Taak registreren: {TASK_NAME}")
    print(f"  Tijdstip:  dagelijks om {schedule_time}")
    print(f"  Script:    {BAT_FILE}")
    print(f"  Commando:  {' '.join(cmd)}")
    print()

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, shell=True
        )
        if result.returncode == 0:
            print("Taak succesvol geregistreerd!")
            print()
            print("Controleer met:")
            print(f"  python scheduler/task_scheduler_setup.py --status")
            print()
            print("Of open Windows Task Scheduler:")
            print(f"  Zoek naar: {TASK_NAME}")
        else:
            print(f"FOUT bij registreren (exit code {result.returncode}):")
            print(result.stderr or result.stdout)
            if "toegang" in (result.stderr + result.stdout).lower() or \
               "access" in (result.stderr + result.stdout).lower():
                print("\nProbeer opnieuw als administrator.")
    except Exception as e:
        print(f"FOUT: {e}")
        sys.exit(1)


def uninstall_task():
    """Verwijder de taak uit Windows Task Scheduler."""
    cmd = ["schtasks", "/delete", "/tn", TASK_NAME, "/f"]
    print(f"Taak verwijderen: {TASK_NAME}")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, shell=True
        )
        if result.returncode == 0:
            print("Taak succesvol verwijderd.")
        else:
            print(f"Kon taak niet verwijderen: {result.stderr or result.stdout}")
    except Exception as e:
        print(f"FOUT: {e}")


def show_status():
    """Toon de status van de geregistreerde taak."""
    cmd = ["schtasks", "/query", "/tn", TASK_NAME, "/v", "/fo", "LIST"]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, shell=True
        )
        if result.returncode == 0:
            print(f"Taak '{TASK_NAME}' gevonden:\n")
            # Filter relevante velden
            for line in result.stdout.splitlines():
                line = line.strip()
                if any(key in line.lower() for key in [
                    "taaknaam", "task name",
                    "status", "volgende", "next run",
                    "laatste", "last run", "result",
                    "schema", "schedule", "start",
                    "auteur", "author",
                ]):
                    print(f"  {line}")
        else:
            print(f"Taak '{TASK_NAME}' niet gevonden.")
            print("Registreer met: python scheduler/task_scheduler_setup.py --install")
    except Exception as e:
        print(f"FOUT: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Windows Task Scheduler setup voor Concurrentiecheck"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--install", action="store_true",
        help="Registreer dagelijkse taak",
    )
    group.add_argument(
        "--uninstall", action="store_true",
        help="Verwijder dagelijkse taak",
    )
    group.add_argument(
        "--status", action="store_true",
        help="Toon status van de taak",
    )
    parser.add_argument(
        "--time", default="07:00",
        help="Tijdstip voor dagelijkse run (HH:MM, default: 07:00)",
    )
    args = parser.parse_args()

    if args.install:
        install_task(schedule_time=args.time)
    elif args.uninstall:
        uninstall_task()
    elif args.status:
        show_status()


if __name__ == "__main__":
    main()

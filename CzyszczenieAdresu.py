#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CzyszczenieAdresu.py
ORCHESTRATOR czyszczenia adresów.

Kolejność:
1) czyszczenieadresu1.py
   - normalizacja
   - usuwanie starych nazw
   - zamiany stare -> nowe (jeśli mapping)

2) czyszczenieadresu2.py
   - uzupełnianie adresów z teryt.csv
   - jeśli brak adresu -> wpis 'brak adresu' do kolumn cenowych
"""

from __future__ import annotations
import argparse
import subprocess
import sys
from pathlib import Path


# =========================
# helpers
# =========================

def run_step(cmd: list[str], step_name: str) -> None:
    print(f"\n=== [{step_name}] START ===")
    print("CMD:", " ".join(cmd))

    result = subprocess.run(
        cmd,
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"[{step_name}] zakończony BŁĘDEM (code={result.returncode})")

    print(f"=== [{step_name}] OK ===")


def resolve_script(name: str) -> Path:
    """
    Szuka skryptu:
    - obok CzyszczenieAdresu.py
    - albo w cwd
    """
    here = Path(__file__).resolve().parent
    p1 = here / name
    if p1.exists():
        return p1

    p2 = Path.cwd() / name
    if p2.exists():
        return p2

    raise FileNotFoundError(f"Nie znaleziono skryptu: {name}")


# =========================
# main
# =========================

def main():
    ap = argparse.ArgumentParser(
        description="Pipeline czyszczenia adresów (etap 1 -> etap 2)"
    )
    ap.add_argument("raport", help="Ścieżka do pliku raportu .xlsx")
    ap.add_argument("--teryt", default="teryt.csv", help="Ścieżka do teryt.csv")
    ap.add_argument("--obszar", default="obszar_sadow.xlsx", help="Ścieżka do obszar_sadow.xlsx")
    ap.add_argument("--mapping", default=None, help="CSV/XLSX ze starymi -> nowymi nazwami (opcjonalnie)")

    args = ap.parse_args()

    raport = Path(args.raport).resolve()
    if not raport.exists():
        raise FileNotFoundError(f"Plik raportu nie istnieje: {raport}")

    py = sys.executable

    # ---------- ETAP 1 ----------
    step1 = resolve_script("czyszczenieadresu1.py")
    cmd1 = [
        py,
        str(step1),
        str(raport),
    ]
    if args.mapping:
        cmd1 += ["--mapping", args.mapping]

    run_step(cmd1, "CZYSZCZENIE ADRESU – ETAP 1")

    # ---------- ETAP 2 ----------
    step2 = resolve_script("czyszczenieadresu2.py")
    cmd2 = [
        py,
        str(step2),
        str(raport),
        "--teryt", args.teryt,
        "--obszar", args.obszar,
    ]

    run_step(cmd2, "CZYSZCZENIE ADRESU – ETAP 2")

    print("\n✔ PIPELINE CZYSZCZENIA ADRESÓW ZAKOŃCZONY SUKCESEM")
    print(f"✔ Plik wynikowy: {raport}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[BŁĄD KRYTYCZNY]: {e}")
        sys.exit(1)

# bootstrap_files.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import csv

VOIVODESHIPS = [
    "Dolnośląskie","Kujawsko-Pomorskie","Lubelskie","Lubuskie","Łódzkie",
    "Małopolskie","Mazowieckie","Opolskie","Podkarpackie","Podlaskie",
    "Pomorskie","Śląskie","Świętokrzyskie","Warmińsko-Mazurskie","Wielkopolskie",
    "Zachodniopomorskie",
]

LINKS_DIR_NAME = "../../linki"
OUT_DIR_NAME   = "województwa"
TIMING_FILE    = "timing.csv"

__all__ = ["prepare_structure", "prepare_app"]


def _ensure_empty_csv(path: Path) -> None:
    """
    Tworzy pusty CSV, jeśli nie istnieje, z nagłówkiem 'link' dla plików z linkami
    oraz pełnym nagłówkiem dla plików wynikowych z ogłoszeniami.
    """
    if path.exists():
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    if path.parent.name == LINKS_DIR_NAME:
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["link"])
        return

    # Pliki danych (województwa)
    headers = [
        "cena","cena_za_metr","metry","liczba_pokoi","pietro","rynek","rok_budowy",
        "material","wojewodztwo","powiat","gmina","miejscowosc","dzielnica","ulica","link",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(headers)


def prepare_structure(base_dir: str | Path) -> Path:
    """
    Tworzy w podanej lokalizacji strukturę:
      <base>/linki/<Województwo>.csv
      <base>/województwa/<Województwo>.csv
      <base>/timing.csv (jeśli nie istnieje)

    Nie usuwa istniejących plików. Zwraca ścieżkę bazową jako Path.
    """
    base = Path(base_dir).expanduser().resolve()
    base.mkdir(parents=True, exist_ok=True)

    links_dir = base / LINKS_DIR_NAME
    out_dir   = base / OUT_DIR_NAME
    links_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    # CSV dla wszystkich województw w obu folderach
    for v in VOIVODESHIPS:
        _ensure_empty_csv(links_dir / f"{v}.csv")
        _ensure_empty_csv(out_dir   / f"{v}.csv")

    # timing.csv (utwórz z nagłówkiem, jeśli brak)
    timing = base / TIMING_FILE
    if not timing.exists():
        with timing.open("w", encoding="utf-8-sig", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["ts","region","phase","status","note","last_index","total"])

    return base


# Alias dla kompatybilności z istniejącym kodem
def prepare_app(base_dir: str | Path) -> Path:
    """Alias do prepare_structure – zostawiony dla zgodności z importami."""
    return prepare_structure(base_dir)

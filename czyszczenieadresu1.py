#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
from pathlib import Path
import re
import unicodedata
import pandas as pd

# ============================================================
# KONFIG
# ============================================================

RAPORT_SHEET = "raport"
MISSING_TOKENS = {"---", "--", "—", "-", "brak", "brak danych", "nan", "none", ""}

# ============================================================
# MAPA: stare województwa (49) -> nowe (16)
# KLUCZE: po norm_key()
# WARTOŚCI: ZAWSZE UPPERCASE
# ============================================================

HIST = {
    # MAZOWIECKIE
    "plockie": "MAZOWIECKIE",
    "warszawskie": "MAZOWIECKIE",
    "ciechanowskie": "MAZOWIECKIE",
    "ostroleckie": "MAZOWIECKIE",
    "siedleckie": "MAZOWIECKIE",
    "radomskie": "MAZOWIECKIE",

    # DOLNOŚLĄSKIE
    "wroclawskie": "DOLNOŚLĄSKIE",
    "jeleniogorskie": "DOLNOŚLĄSKIE",
    "walbrzyskie": "DOLNOŚLĄSKIE",
    "legnickie": "DOLNOŚLĄSKIE",

    # KUJAWSKO-POMORSKIE
    "bydgoskie": "KUJAWSKO-POMORSKIE",
    "torunskie": "KUJAWSKO-POMORSKIE",
    "wloclawskie": "KUJAWSKO-POMORSKIE",

    # LUBELSKIE
    "lubelskie": "LUBELSKIE",
    "bialskopodlaskie": "LUBELSKIE",
    "chelmskie": "LUBELSKIE",
    "zamojskie": "LUBELSKIE",

    # LUBUSKIE
    "zielonogorskie": "LUBUSKIE",
    "gorzowskie": "LUBUSKIE",

    # ŁÓDZKIE
    "lodzkie": "ŁÓDZKIE",
    "piotrkowskie": "ŁÓDZKIE",
    "sieradzkie": "ŁÓDZKIE",
    "skierniewickie": "ŁÓDZKIE",

    # MAŁOPOLSKIE
    "krakowskie": "MAŁOPOLSKIE",
    "tarnowskie": "MAŁOPOLSKIE",
    "nowosadeckie": "MAŁOPOLSKIE",

    # OPOLSKIE
    "opolskie": "OPOLSKIE",

    # PODKARPACKIE
    "rzeszowskie": "PODKARPACKIE",
    "przemyskie": "PODKARPACKIE",
    "krosnienskie": "PODKARPACKIE",
    "tarnobrzeskie": "PODKARPACKIE",

    # PODLASKIE
    "bialostockie": "PODLASKIE",
    "lomzynskie": "PODLASKIE",
    "suwalskie": "PODLASKIE",

    # POMORSKIE
    "gdanskie": "POMORSKIE",
    "slupskie": "POMORSKIE",

    # ŚLĄSKIE
    "katowickie": "ŚLĄSKIE",
    "bielskie": "ŚLĄSKIE",
    "czestochowskie": "ŚLĄSKIE",

    # ŚWIĘTOKRZYSKIE
    "kieleckie": "ŚWIĘTOKRZYSKIE",

    # WARMIŃSKO-MAZURSKIE
    "olsztynskie": "WARMIŃSKO-MAZURSKIE",
    "elblaskie": "WARMIŃSKO-MAZURSKIE",

    # WIELKOPOLSKIE
    "poznanskie": "WIELKOPOLSKIE",
    "kaliskie": "WIELKOPOLSKIE",
    "koninskie": "WIELKOPOLSKIE",
    "leszczynskie": "WIELKOPOLSKIE",
    "pilskie": "WIELKOPOLSKIE",

    # ZACHODNIOPOMORSKIE
    "szczecinskie": "ZACHODNIOPOMORSKIE",
    "koszalinskie": "ZACHODNIOPOMORSKIE",
}

# warianty typu "WOJEWÓDZTWO PŁOCKIE"
for k, v in list(HIST.items()):
    HIST[f"wojewodztwo {k}"] = v
    HIST[f"{k} wojewodztwo"] = v

HIST_KEYS_DESC = sorted(HIST.keys(), key=len, reverse=True)

# ============================================================
# AKTUALNE WOJEWÓDZTWA (16) — żeby nie łapać 'opolskie' w 'wielkopolskie'
# Klucze: po norm_key() (bez ogonków, lower, spacje)
# Wartości: oficjalne nazwy UPPERCASE (z polskimi znakami)
# ============================================================

CURRENT_16 = {
    "dolnoslaskie": "DOLNOŚLĄSKIE",
    "kujawsko pomorskie": "KUJAWSKO-POMORSKIE",
    "lubelskie": "LUBELSKIE",
    "lubuskie": "LUBUSKIE",
    "lodzkie": "ŁÓDZKIE",
    "malopolskie": "MAŁOPOLSKIE",
    "mazowieckie": "MAZOWIECKIE",
    "opolskie": "OPOLSKIE",
    "podkarpackie": "PODKARPACKIE",
    "podlaskie": "PODLASKIE",
    "pomorskie": "POMORSKIE",
    "slaskie": "ŚLĄSKIE",
    "swietokrzyskie": "ŚWIĘTOKRZYSKIE",
    "warminsko mazurskie": "WARMIŃSKO-MAZURSKIE",
    "wielkopolskie": "WIELKOPOLSKIE",
    "zachodniopomorskie": "ZACHODNIOPOMORSKIE",
}

# warianty typu "WOJEWÓDZTWO WIELKOPOLSKIE" / "WIELKOPOLSKIE WOJEWÓDZTWO"
for k, v in list(CURRENT_16.items()):
    CURRENT_16[f"wojewodztwo {k}"] = v
    CURRENT_16[f"{k} wojewodztwo"] = v


# ============================================================
# NORMALIZACJA — KLUCZOWY FIX DLA Ł/ł
# ============================================================

PL_TRANSLATE = str.maketrans({
    "ą": "a", "ć": "c", "ę": "e", "ł": "l", "ń": "n", "ó": "o", "ś": "s", "ż": "z", "ź": "z",
    "Ą": "a", "Ć": "c", "Ę": "e", "Ł": "l", "Ń": "n", "Ó": "o", "Ś": "s", "Ż": "z", "Ź": "z",
})

def norm_missing(x):
    if x is None:
        return None
    s = str(x).strip()
    return None if s.lower() in MISSING_TOKENS else s


def norm_key(s: str | None) -> str:
    """
    PŁOCKIE -> plockie
    WOJ. PŁOCKIE -> woj plockie
    """
    s = str(s or "").strip().lower()
    s = s.translate(PL_TRANSLATE)   # <-- KLUCZOWE
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def upper_or_none(x):
    x = norm_missing(x)
    if x is None:
        return None
    return str(x).strip().upper()


def replace_historical_voivodeship(val: str | None) -> str | None:
    val = norm_missing(val)
    if val is None:
        return None

    key = norm_key(val)

    # exact
    if key in HIST:
        return HIST[key]

    # bez prefiksu "woj/wojewodztwo"
    key2 = re.sub(r"^(woj|wojewodztwo)\s+", "", key).strip()

    # jeżeli użytkownik podał już aktualne województwo (np. WIELKOPOLSKIE / MAŁOPOLSKIE),
    # zwróć oficjalną nazwę i NIE rób 'contains' (bo 'opolskie' jest podciągiem obu).
    key3 = re.sub(r"\s+(woj|wojewodztwo)$", "", key).strip()
    if key in CURRENT_16:
        return CURRENT_16[key]
    if key2 in CURRENT_16:
        return CURRENT_16[key2]
    if key3 in CURRENT_16:
        return CURRENT_16[key3]

    if key2 in HIST:
        return HIST[key2]

    # contains
    for old in HIST_KEYS_DESC:
        if old:
            # dopasuj tylko całe słowo/frazę (a nie podciąg w środku słowa)
            if re.search(rf"(^|\s){re.escape(old)}($|\s)", key):
                return HIST[old]

    return val.upper()


# ============================================================
# IO — tylko arkusz 'raport'
# ============================================================

def read_df(path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(path, engine="openpyxl")
    sheet = RAPORT_SHEET if RAPORT_SHEET in xl.sheet_names else xl.sheet_names[0]
    return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")


def write_replace_raport(path: Path, df: pd.DataFrame) -> None:
    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name=RAPORT_SHEET, index=False)


# ============================================================
# MAIN
# ============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("raport", help="Plik XLSX/XLSM")
    args = ap.parse_args()

    raport = Path(args.raport).resolve()
    df = read_df(raport)

    if "Województwo" not in df.columns:
        raise RuntimeError("Brak kolumny 'Województwo'.")

    before = df["Województwo"].map(upper_or_none)

    df["Województwo"] = (
        df["Województwo"]
        .apply(replace_historical_voivodeship)
        .map(upper_or_none)
    )

    after = df["Województwo"]
    changed = (before.fillna("") != after.fillna("")).sum()

    write_replace_raport(raport, df)
    print(f"✔ ETAP 1 OK — ZMIENIONO {changed} wierszy (PŁOCKIE → MAZOWIECKIE działa)")


if __name__ == "__main__":
    main()

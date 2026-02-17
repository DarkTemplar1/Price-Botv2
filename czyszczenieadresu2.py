#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
czyszczenieadresu2.py  (ETAP 2)

- uzupełnia: Województwo / Powiat / Gmina / Miejscowość
- źródła: teryt.csv + obszar_sadow.xlsx
- działa in-place: NADPISUJE TYLKO arkusz 'raport'
- pełny UPPERCASE wyników

DODANE:
- rozpoznanie miast wojewódzkich (w tym podwójne stolice)
- jeśli wykryto miasto wojewódzkie => auto-uzupełnij WOJ, a POW/GMI dobierz z TERYT
"""

from __future__ import annotations
import argparse
import re
import unicodedata
from pathlib import Path
import pandas as pd


# =========================
# KONFIG
# =========================

RAPORT_SHEET = "raport"

COL_WOJ = "Województwo"
COL_POW = "Powiat"
COL_GMI = "Gmina"
COL_MIA = "Miejscowość"
COL_KW = "Nr KW"

HINT_COLS = [
    "_addr_hint",
    "Cały adres (dla lokalu)",
    "Położenie",
    "Ulica(dla lokalu)",
    "Ulica(dla budynku)",
    "Ulica",
    "Dzielnica",
    "Miejscowość",
]

MISSING_TOKENS = {"---", "--", "—", "-", "brak", "brak danych", "nan", "none", ""}

COL_DZI = "Dzielnica"

# Prefiksy administracyjne występujące w danych (np. "M. Poznań", "M. ST. Warszawa")
RE_M_ST = re.compile(r"(?i)\bm\.?\s*st\.?\b")
RE_M = re.compile(r"(?i)\bm\.?\b")
RE_DIGITS = re.compile(r"\d+")

def clean_admin_cell(x):
    """Usuwa 'M.' / 'M. ST.' oraz cyfry z komórek administracyjnych (woj/pow/gmi/mia/dzielnica)."""
    x = norm_missing(x)
    if x is None:
        return None
    s = str(x)
    # usuń prefiksy typu "M." / "M. ST."
    s = RE_M_ST.sub(" ", s)
    s = RE_M.sub(" ", s)
    # usuń cyfry (np. numery budynków, kody pocztowe w polu miejscowości)
    s = RE_DIGITS.sub(" ", s)
    # porządki
    s = re.sub(r"[()\[\]{}]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None


# =========================
# Miasta wojewódzkie / stolice
# =========================
# Klucze porównujemy po norm_key() (bez ogonków, lower)
# Wartości: (miasto, województwo) — oba potem zapisujemy UPPERCASE
CAPITAL_CITY_TO_VOIV = {
    # dolnośląskie
    "wroclaw": ("WROCŁAW", "DOLNOŚLĄSKIE"),

    # kujawsko-pomorskie (2 stolice)
    "bydgoszcz": ("BYDGOSZCZ", "KUJAWSKO-POMORSKIE"),
    "torun": ("TORUŃ", "KUJAWSKO-POMORSKIE"),

    # lubelskie
    "lublin": ("LUBLIN", "LUBELSKIE"),

    # lubuskie (2 stolice)
    "gorzow wielkopolski": ("GORZÓW WIELKOPOLSKI", "LUBUSKIE"),
    "gorzow wlkp": ("GORZÓW WIELKOPOLSKI", "LUBUSKIE"),
    "zielona gora": ("ZIELONA GÓRA", "LUBUSKIE"),

    # łódzkie
    "lodz": ("ŁÓDŹ", "ŁÓDZKIE"),

    # małopolskie
    "krakow": ("KRAKÓW", "MAŁOPOLSKIE"),

    # mazowieckie
    "warszawa": ("WARSZAWA", "MAZOWIECKIE"),

    # opolskie
    "opole": ("OPOLE", "OPOLSKIE"),

    # podkarpackie
    "rzeszow": ("RZESZÓW", "PODKARPACKIE"),

    # podlaskie
    "bialystok": ("BIAŁYSTOK", "PODLASKIE"),

    # pomorskie
    "gdansk": ("GDAŃSK", "POMORSKIE"),

    # śląskie
    "katowice": ("KATOWICE", "ŚLĄSKIE"),

    # świętokrzyskie
    "kielce": ("KIELCE", "ŚWIĘTOKRZYSKIE"),

    # warmińsko-mazurskie
    "olsztyn": ("OLSZTYN", "WARMIŃSKO-MAZURSKIE"),

    # wielkopolskie
    "poznan": ("POZNAŃ", "WIELKOPOLSKIE"),

    # zachodniopomorskie
    "szczecin": ("SZCZECIN", "ZACHODNIOPOMORSKIE"),
}


# =========================
# NORMALIZACJA
# =========================

def norm_missing(x):
    if x is None:
        return None
    s = str(x).strip()
    return None if s.lower() in MISSING_TOKENS else s


def upper_or_none(x):
    if x is None:
        return None
    s = str(x).strip()
    return s.upper() if s else None


def norm_key(s: str | None) -> str:
    """bez ogonków, lower, 1 spacja + usuwa M./M.ST oraz cyfry"""
    s = str(s or "").strip().lower()
    # usuń prefiksy 'm.' / 'm. st.' oraz cyfry (dla dopasowań administracyjnych)
    s = RE_M_ST.sub(" ", s)
    s = RE_M.sub(" ", s)
    s = RE_DIGITS.sub(" ", s)
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    s = re.sub(r"[^a-z0-9\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# =========================
# KW -> kod sądu
# =========================

KW_COURT_RE = re.compile(r"^\s*([A-Z]{2}\d[A-Z])\s*/", re.I)

def extract_court_code(nr_kw: str | None) -> str | None:
    if not nr_kw:
        return None
    m = KW_COURT_RE.match(str(nr_kw).strip().upper())
    return m.group(1) if m else None


# =========================
# Hint / tekst
# =========================

SPLIT_RE = re.compile(r"[|,;]+")

def build_hint_text(row: pd.Series) -> str:
    parts = []
    for c in HINT_COLS:
        if c in row.index:
            v = norm_missing(row[c])
            if v:
                parts.append(str(v))
    return " | ".join(parts)


def detect_capital_from_text(text: str) -> tuple[str, str] | None:
    """
    Szuka miasta wojewódzkiego w dowolnym tekście (adres/hint/miejscowość).
    Zwraca (MIASTO_UPPER, WOJ_UPPER) jeśli wykryje.
    """
    h = norm_key(text)
    if not h:
        return None

    # 1) dokładne dopasowanie całego stringa
    if h in CAPITAL_CITY_TO_VOIV:
        return CAPITAL_CITY_TO_VOIV[h]

    # 2) dopasowanie fraz w środku (tokeny)
    # skanuj dłuższe najpierw (np. gorzow wielkopolski)
    keys = sorted(CAPITAL_CITY_TO_VOIV.keys(), key=len, reverse=True)
    for k in keys:
        if k and k in h:
            return CAPITAL_CITY_TO_VOIV[k]

    return None


def guess_miejscowosc_from_hint(hint: str, miejsc_keys_set: set[str], miejsc_key_to_canon: dict[str, str]) -> str | None:
    """
    Heurystyka: znajdź nazwę miejscowości z TERYT w tekście hint.
    """
    h = norm_key(hint)
    if not h:
        return None

    segs = [norm_key(s) for s in SPLIT_RE.split(hint) if norm_key(s)]
    for seg in reversed(segs):
        seg = re.sub(r"^(ul|ulica|al|aleja|os|osiedle|pl|plac)\.?\s+", "", seg).strip()
        if seg in miejsc_keys_set:
            return miejsc_key_to_canon[seg]

    words = [w for w in re.split(r"\s+", h) if w]
    for n in (4, 3, 2, 1):
        if len(words) < n:
            continue
        for i in range(0, len(words) - n + 1):
            phrase = " ".join(words[i:i+n]).strip()
            if phrase in miejsc_keys_set:
                return miejsc_key_to_canon[phrase]

    return None


# =========================
# TERYT + OBSZAR SĄDÓW
# =========================

def load_teryt(path: Path) -> pd.DataFrame:
    # sep=";" bo często tak masz w PL
    df = pd.read_csv(path, sep=";", encoding="utf-8", engine="python")

    # spodziewane kolumny: Wojewodztwo, Powiat, Gmina, Miejscowosc, Dzielnica
    # zostawiamy jako stringi (bez uppercase na tym etapie – uppercase robimy przy zapisie)
    for c in ["Wojewodztwo", "Powiat", "Gmina", "Miejscowosc", "Dzielnica"]:
        if c in df.columns:
            df[c] = df[c].astype(str).map(lambda x: x.strip() if x and x != "nan" else "")
    return df


def load_obszar(path: Path) -> pd.DataFrame:
    return pd.read_excel(path)


def build_teryt_index(teryt: pd.DataFrame):
    """
    miejscowość_key -> lista rekordów (woj, pow, gmi) + kanoniczna nazwa miejscowości
    """
    miejsc_key_to_canon: dict[str, str] = {}
    miejsc_to_rows: dict[str, list[tuple[str, str, str]]] = {}

    for _, r in teryt.iterrows():
        miejsc = (r.get("Miejscowosc") or "").strip()
        woj = (r.get("Wojewodztwo") or "").strip()
        powiat = (r.get("Powiat") or "").strip()
        gmina = (r.get("Gmina") or "").strip()

        if not miejsc:
            continue

        k = norm_key(miejsc)
        if not k:
            continue

        miejsc_key_to_canon.setdefault(k, miejsc)
        miejsc_to_rows.setdefault(k, []).append((woj, powiat, gmina))

    return set(miejsc_key_to_canon.keys()), miejsc_key_to_canon, miejsc_to_rows


def build_obszar_index(obs: pd.DataFrame):
    """
    indeks:
    - court_code -> unique woj (jeśli jednoznaczne)
    """
    court_to_woj: dict[str, str] = {}
    if "Oznaczenie sądu" not in obs.columns:
        return court_to_woj

    for code, g in obs.groupby("Oznaczenie sądu"):
        code = str(code).strip().upper()
        woj_vals = []
        if "Województwo" in g.columns:
            woj_vals = [str(x).strip() for x in g["Województwo"].dropna().unique().tolist()]
            woj_vals = [w for w in woj_vals if w and w.lower() != "nan"]
        if len(set(woj_vals)) == 1:
            court_to_woj[code] = woj_vals[0]

    return court_to_woj


def fill_from_teryt(miejsc_key: str, wanted_woj: str | None, miejsc_to_rows: dict[str, list[tuple[str, str, str]]]):
    """
    Zwraca (woj, powiat, gmina) dla danej miejscowości.
    Jeśli wanted_woj podany => wybiera rekord pasujący województwem.
    """
    rows = miejsc_to_rows.get(miejsc_key, [])
    if not rows:
        return None, None, None

    if wanted_woj:
        w = norm_key(wanted_woj)
        filtered = [r for r in rows if norm_key(r[0]) == w]
        if filtered:
            rows = filtered

    woj, powiat, gmina = rows[0]
    return woj or None, powiat or None, gmina or None


# =========================
# ZAPIS: nadpisz tylko arkusz 'raport'
# =========================

def read_report_df(xlsx: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx, engine="openpyxl")
    sheet = RAPORT_SHEET if RAPORT_SHEET in xl.sheet_names else xl.sheet_names[0]
    return pd.read_excel(xlsx, sheet_name=sheet, engine="openpyxl")


def write_replace_raport_sheet(xlsx: Path, df: pd.DataFrame) -> None:
    with pd.ExcelWriter(
        xlsx,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:
        df.to_excel(writer, sheet_name=RAPORT_SHEET, index=False)


# =========================
# MAIN
# =========================

def main():
    ap = argparse.ArgumentParser(description="ETAP 2 – uzupełnianie adresów (UPPERCASE + miasta wojewódzkie)")
    ap.add_argument("raport", help="Plik raportu .xlsx/.xlsm")
    ap.add_argument("--teryt", default="teryt.csv", help="Ścieżka do teryt.csv")
    ap.add_argument("--obszar", default="obszar_sadow.xlsx", help="Ścieżka do obszar_sadow.xlsx")
    args = ap.parse_args()

    raport = Path(args.raport).resolve()
    teryt_path = Path(args.teryt).resolve()
    obszar_path = Path(args.obszar).resolve()

    if not raport.exists():
        raise FileNotFoundError(f"Brak raportu: {raport}")
    if not teryt_path.exists():
        raise FileNotFoundError(f"Brak teryt.csv: {teryt_path}")
    if not obszar_path.exists():
        raise FileNotFoundError(f"Brak obszar_sadow.xlsx: {obszar_path}")

    df = read_report_df(raport)

    teryt = load_teryt(teryt_path)
    obs = load_obszar(obszar_path)

    miejsc_keys_set, miejsc_key_to_canon, miejsc_to_rows = build_teryt_index(teryt)
    court_to_woj = build_obszar_index(obs)

    # zapewnij kolumny docelowe
    for c in [COL_WOJ, COL_POW, COL_GMI, COL_MIA]:
        if c not in df.columns:
            df[c] = None

    # normalize missing in target cols (ale NIE upper tu jeszcze — upper robimy na zapisie)
    for c in [COL_WOJ, COL_POW, COL_GMI, COL_MIA]:
        df[c] = df[c].map(norm_missing)

    # (1) Usuń prefiksy 'M.' / 'M. ST.' oraz (2) cyfry z komórek adresowych
    for c in [COL_WOJ, COL_POW, COL_GMI, COL_MIA]:
        df[c] = df[c].map(clean_admin_cell)
    if COL_DZI in df.columns:
        df[COL_DZI] = df[COL_DZI].map(clean_admin_cell)

    if COL_KW in df.columns:
        df[COL_KW] = df[COL_KW].map(lambda x: str(x).strip() if pd.notna(x) else "")

    filled_woj = filled_pow = filled_gmi = filled_mia = 0
    capital_hits = 0

    for i in range(len(df)):
        row = df.iloc[i]

        woj = norm_missing(row.get(COL_WOJ))
        powiat = norm_missing(row.get(COL_POW))
        gmina = norm_missing(row.get(COL_GMI))
        miejsc = norm_missing(row.get(COL_MIA))

        court_code = extract_court_code(row.get(COL_KW)) if COL_KW in df.columns else None
        hint = build_hint_text(row)

        # 0) ROZPOZNAJ MIASTO WOJEWÓDZKIE (miejscowość albo hint)
        cap = None
        if miejsc:
            cap = detect_capital_from_text(miejsc)
        if not cap:
            cap = detect_capital_from_text(hint)

        if cap:
            cap_city, cap_woj = cap
            # ustaw miejscowość jeśli pusta
            if not miejsc:
                miejsc = cap_city
                df.at[i, COL_MIA] = upper_or_none(miejsc)
                filled_mia += 1
            # ustaw woj jeśli puste
            if not woj:
                woj = cap_woj
                df.at[i, COL_WOJ] = upper_or_none(woj)
                filled_woj += 1
            capital_hits += 1

        # 1) jeśli brak woj, a mamy sąd i woj jednoznaczne dla sądu
        if not woj and court_code and court_code in court_to_woj:
            woj = court_to_woj[court_code]
            df.at[i, COL_WOJ] = upper_or_none(woj)
            filled_woj += 1

        # 2) jeśli brak miejscowości – spróbuj wyciągnąć z hintu (TERYT)
        if not miejsc:
            guess = guess_miejscowosc_from_hint(hint, miejsc_keys_set, miejsc_key_to_canon)
            if guess:
                miejsc = guess
                df.at[i, COL_MIA] = upper_or_none(miejsc)
                filled_mia += 1

        # 3) jeśli mamy miejscowość – dobierz powiat/gminę (i ewentualnie woj) z TERYT
        if miejsc:
            mk = norm_key(miejsc)
            woj_from_teryt, pow_from_teryt, gmi_from_teryt = fill_from_teryt(
                mk,
                wanted_woj=woj,
                miejsc_to_rows=miejsc_to_rows
            )

            if not woj and woj_from_teryt:
                woj = woj_from_teryt
                df.at[i, COL_WOJ] = upper_or_none(woj)
                filled_woj += 1

            if not powiat and pow_from_teryt:
                powiat = pow_from_teryt
                df.at[i, COL_POW] = upper_or_none(powiat)
                filled_pow += 1

            if not gmina and gmi_from_teryt:
                gmina = gmi_from_teryt
                df.at[i, COL_GMI] = upper_or_none(gmina)
                filled_gmi += 1

        # 4) finalny przymus UPPERCASE nawet jeśli coś przyszło “z ręki”
        df.at[i, COL_WOJ] = upper_or_none(df.at[i, COL_WOJ])
        df.at[i, COL_POW] = upper_or_none(df.at[i, COL_POW])
        df.at[i, COL_GMI] = upper_or_none(df.at[i, COL_GMI])
        df.at[i, COL_MIA] = upper_or_none(df.at[i, COL_MIA])

    # ZAPIS: nadpisz tylko arkusz 'raport'
    write_replace_raport_sheet(raport, df)

    print("✔ ETAP 2 ZAKOŃCZONY (UPPERCASE)")
    print(f"  raport: {raport}")
    print(f"  uzupełniono: WOJ={filled_woj}, POW={filled_pow}, GMI={filled_gmi}, MIA={filled_mia}")
    print(f"  wykrycia miast wojewódzkich: {capital_hits}")


if __name__ == "__main__":
    main()

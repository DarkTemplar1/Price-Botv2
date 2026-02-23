#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''
automat.py â Wersja B (BDL + lokalna ludnoĹÄ + bezpieczny zapis arkusza 'raport')

FIX (17.12.2025+):
- ludnosc.csv jest wczytywane OK (logi), ale brak trafieĹ wynikaĹ z rĂłĹźnic w nazwach (pow./powiat, gmina miejska..., nawiasy)
- dodano kanonizacjÄ nazw jednostek (usuwa prefiksy/skrĂłtowce/nawiasy)
- dodano fallback dopasowania po (woj + miejscowosc) + preferencja dzielnicy
- zapis XLSX: openpyxl, tylko arkusz 'raport' (bez kasowania innych arkuszy)
'''

from pathlib import Path
import sys
import unicodedata
import csv
import os
import datetime
import re
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass

import pandas as pd
import numpy as np


def _filter_outliers_df(df, price_col: str):
    """Zawsze usuwa wartoĹci brzegowe z prĂłby cen (dla wyliczeĹ).

    Zasada:
    - n<=2: nie da siÄ sensownie przyciÄÄ -> zwracamy bez zmian
    - n=3..4: usuwamy min i max (zostajÄ wartoĹci Ĺrodkowe)
    - n>=5: filtr IQR (1.5*IQR); jeĹli da zbyt maĹo danych -> fallback do min/max
    """
    import numpy as _np

    if df is None or len(df.index) == 0:
        return df, _np.array([], dtype=float)

    prices_all = df[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan)
    valid = prices_all.dropna()
    n = int(len(valid))
    if n <= 2:
        return df, valid.to_numpy(dtype=float)

    # MaĹe prĂłby: obetnij skrajne wartoĹci (min/max)
    if n <= 4:
        order = valid.sort_values()
        keep_idx = order.iloc[1:-1].index
        df2 = df.loc[keep_idx].copy()
        prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()
        return df2, prices2.to_numpy(dtype=float)

    # IQR
    q1 = _np.nanpercentile(valid, 25)
    q3 = _np.nanpercentile(valid, 75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr

    mask = (prices_all >= lo) & (prices_all <= hi)
    df2 = df[mask].copy()
    prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()

    # JeĹli filtr IQR wyciÄĹ prawie wszystko, wrĂłÄ do prostego min/max
    if len(prices2) < 2:
        order = valid.sort_values()
        keep_idx = order.iloc[1:-1].index
        df2 = df.loc[keep_idx].copy()
        prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()

    return df2, prices2.to_numpy(dtype=float)

    q1 = _np.nanpercentile(valid, 25)
    q3 = _np.nanpercentile(valid, 75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr

    mask = (prices_all >= lo) & (prices_all <= hi)
    df2 = df[mask].copy()
    prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()

    if len(prices2) < 2:
        order = valid.sort_values()
        keep_idx = order.iloc[1:-1].index
        df2 = df.loc[keep_idx].copy()
        prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()

    return df2, prices2.to_numpy(dtype=float)


import importlib.util
import requests
from pathlib import Path


def import_local_automat():
    here = Path(__file__).resolve().parent
    p = here / "automat.py"
    spec = importlib.util.spec_from_file_location("automat", str(p))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# =========================
# Helpers
# =========================

def _norm(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "").replace("\xa0", "").replace("\t", "")


def _plain(x) -> str:
    """Bezpieczna normalizacja tekstu dla dowolnego typu (str/float/None/NaN)."""
    if x is None:
        return ""
    try:
        if isinstance(x, float) and np.isnan(x):
            return ""
    except Exception:
        pass

    s = str(x).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = " ".join(s.split())
    return s


def _strip_parentheses(s: str) -> str:
    # usuĹ nawiasy i zawartoĹÄ: "GdaĹsk (miasto)" -> "GdaĹsk"
    return re.sub(r"\([^)]*\)", " ", s).strip()


def _canon_admin(part: str, kind: str) -> str:
    """
    kind: woj/pow/gmi/mia/dzl
    Ujednolica teksty z raportu i csv:
    - usuwa nawiasy
    - usuwa znaki interpunkcyjne
    - usuwa sĹowa typu: powiat, pow., gmina, gm., woj., wojewĂłdztwo, itd.
    """
    s = _plain(part)
    if not s:
        return ""
    s = _strip_parentheses(s)

    # zamieĹ myĹlniki/slashe na spacje
    s = s.replace("-", " ").replace("/", " ")
    # wywal wszystko poza litery/cyfry/spacje
    s = re.sub(r"[^0-9a-z ]+", " ", s)
    s = " ".join(s.split())

    # tokeny do wywalenia
    drop_common = {
        "woj", "woj.", "wojewodztwo",
        "pow", "pow.", "powiat",
        "gmina", "gm", "gm.",
        "miasto", "m", "m.",
        "osiedle", "dzielnica",
        "miejska", "wiejska", "miejskowiejska", "miejsko", "wiejsko",
        "na", "prawach", "powiatu",
    }

    tokens = [t for t in s.split() if t not in drop_common]

    # czasem po usuniÄciu zostaje pusto â wtedy zostaw oryginalne (po plain)
    if not tokens:
        tokens = s.split()

    return " ".join(tokens).strip()


def _find_col(cols, candidates):
    norm_map = {_norm(c): c for c in cols}
    for cand in candidates:
        key = _norm(cand)
        if key in norm_map:
            return norm_map[key]
    for c in cols:
        if any(_norm(x) in _norm(c) for x in candidates):
            return c
    return None


def _trim_after_semicolon(val):
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    s = str(val)
    if ";" in s:
        s = s.split(";", 1)[0]
    return s.strip()


def _to_float_maybe(x):
    if x is None:
        return None
    try:
        if isinstance(x, float) and np.isnan(x):
            return None
    except Exception:
        pass

    s = str(x)
    for unit in ["mÂ˛", "m2", "zĹ/mÂ˛", "zĹ/m2", "zĹ"]:
        s = s.replace(unit, "")
    s = s.replace(" ", "").replace("\xa0", "").replace(",", ".")
    s = "".join(ch for ch in s if (ch.isdigit() or ch == "." or ch == "-"))
    try:
        return float(s) if s else None
    except Exception:
        return None


def _find_ludnosc_csv(baza_folder: Path, raport_path: Path, polska_path: Path) -> Path | None:
    """
    Szukamy *tylko* jednego ĹşrĂłdĹa ludnoĹci: pliku `ludnosc.csv`.

    Priorytet:
      1) folder raportu (tam gdzie jest plik raportu / Polska.xlsx wybierana w GUI)
      2) folder z `Polska.xlsx` (baza)
      3) `baza_folder` przekazany do automatu

    Dodatkowo: jeĹli trafimy na âstaryâ plik (np. ~2k wierszy), ignorujemy go,
    bo powinien byÄ peĹny (~100k rekordĂłw).
    """
    env = os.getenv("LUDNOSC_CSV_PATH")
    candidates: List[Path] = []
    if env:
        candidates.append(Path(env))
    candidates += [
        raport_path.parent / "ludnosc.csv",
        polska_path.parent / "ludnosc.csv",
        baza_folder / "ludnosc.csv",
    ]

    def _looks_full(p: Path) -> bool:
        try:
            with p.open("r", encoding="utf-8-sig", errors="ignore") as f:
                # -1 bo nagĹĂłwek
                n = sum(1 for _ in f) - 1
            return n >= 50000
        except Exception:
            return True  # nie blokuj w razie problemĂłw z odczytem

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                if _looks_full(p):
                    return p.resolve()
        except Exception:
            pass
    return None


# =========================
# Aglomeracja warszawska (miejscowoĹci bez Warszawy)
# =========================
# Uwaga: lista moĹźe byÄ nadpisana zewnÄtrznym plikiem (XLSX/CSV) â patrz load_warsaw_agglomeration().
AGLO_WARSZAWA_DEFAULT = {
    'piaseczno', 'konstancin jeziorna', 'gora kalwaria', 'lesznowola', 'prazmow', 'jozefow', 'otwock', 'celestynow',
    'karczew', 'kolbiel', 'wiazowna',
    'pruszkow', 'piastow', 'brwinow', 'michalowice', 'nadarzyn', 'raszyn',
    'blonie', 'izabelin', 'kampinos', 'leszno', 'stare babice', 'lomianki', 'ozarow mazowiecki',
    'marki', 'zabki', 'zielonka', 'wolomin', 'kobylka', 'radzymin', 'tluszcz', 'jadow', 'dabrowka', 'poswietne',
    'legionowo', 'jablonna', 'nieporet', 'serock', 'wieliszew', 'nowy dwor mazowiecki', 'czosnow', 'leoncin',
    'pomiechowek', 'zakroczym',
    'grodzisk mazowiecki', 'milanowek', 'podkowa lesna'
}

# Zestaw 18 stolic wojewodztw (kanonizowane, bez polskich znakow)
VOIVODE_CAPITALS = {
    'bialystok', 'bydgoszcz', 'torun', 'gdansk', 'gorzow wielkopolski', 'katowice', 'kielce', 'krakow',
    'lublin', 'lodz', 'olsztyn', 'opole', 'poznan', 'rzeszow', 'szczecin', 'warszawa', 'wroclaw', 'zielona gora'
}

from typing import Set
import pandas as _pd
import re as _re


def _canon_local(_s: str) -> str:
    s = str(_s or '').strip().lower()
    s = _re.sub(r"\(.*?\)", " ", s)
    s = s.replace('-', ' ').replace('/', ' ')
    s = ''.join(ch for ch in s if ch.isalnum() or ch.isspace())
    s = ' '.join(s.split())
    return s


def load_warsaw_agglomeration(hint_path: Path | None = None) -> Set[str]:
    # Wczytuje miejscowosci aglomeracji warszawskiej z pliku 'aglomeracja_warszawska.xlsx' (ten sam folder).
    # Jesli nie znajdzie - zwraca zestaw domyslny (AGLO_WARSZAWA_DEFAULT).
    candidates: list[Path] = []
    here = Path(__file__).resolve().parent
    candidates.append(here / 'aglomeracja_warszawska.xlsx')
    if hint_path:
        candidates.append(hint_path.parent / 'aglomeracja_warszawska.xlsx')
    import pandas as _pd
    try:
        for p in candidates:
            if not p.exists():
                continue
            xls = _pd.ExcelFile(p, engine='openpyxl')
            sheet = None
            for nm in xls.sheet_names:
                if 'reszta' in nm.lower() or 'aglo' in nm.lower():
                    sheet = nm
                    break
            if sheet is None:
                sheet = xls.sheet_names[0]
            df = _pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')
            cols = {str(c).lower(): c for c in df.columns}
            mia_col = None
            for key in ['miejsc', 'miejscowosc', 'miejscowoĹÄ', 'miasto']:
                if key in cols:
                    mia_col = cols[key]
                    break
            if mia_col is None:
                mia_col = list(df.columns)[-1]
            vals: set[str] = set()
            for v in df[mia_col].dropna().astype(str):
                c = _canon_local(v)
                if c and c != 'warszawa':
                    vals.add(c)
            if vals:
                return vals
    except Exception:
        pass
    return set(AGLO_WARSZAWA_DEFAULT)


# =========================
# Progi ludnoĹci (domyĹlne)
# (moĹźesz je zmieniaÄ w GUI: "Ustawienia progĂłw ludnoĹci")
# Format: (min_pop, max_pop | None, margin_m2, margin_pct)
# =========================

POP_MARGIN_RULES = [
    (0, 6000, 25.0, 15.0),
    (6000, 20000, 20.0, 15.0),
    (20000, 50000, 20.0, 15.0),
    (50000, 200000, 15.0, 15.0),
    (200000, None, 10.0, 15.0),
]


def rules_for_population(pop):
    if pop is None:
        return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])
    try:
        p = float(pop)
    except Exception:
        return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])

    for low, high, m2, pct in POP_MARGIN_RULES:
        if p >= low and (high is None or p < high):
            return float(m2), float(pct)
    return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])


def _eq_mask(df: pd.DataFrame, col_candidates, value: str) -> pd.Series:
    col = _find_col(df.columns, col_candidates)
    if col is None or not str(value).strip():
        return pd.Series(True, index=df.index)
    s = df[col].astype(str).str.strip().str.lower()
    v = str(value).strip().lower()
    return s == v


# =========================
# BDL / ludnoĹÄ
# =========================

BDL_BASE_URL = "https://bdl.stat.gov.pl/api/v1"
BDL_API_KEY_DEFAULT = "c804c054-f519-45b3-38f3-08de375a07dc"

_BDL_POP_VAR_ID: str | None = None
_BDL_POP_VAR_ID_NOT_FOUND = "__NOT_FOUND__"


def _bdl_headers() -> dict:
    api_key = os.getenv("BDL_API_KEY") or os.getenv("GUS_BDL_API_KEY") or BDL_API_KEY_DEFAULT
    if not api_key:
        return {}
    return {"X-ClientId": api_key, "Accept": "application/json"}


def _pick_latest_year():
    return datetime.date.today().year - 1


class PopulationResolver:
    def __init__(self, local_csv: Path | None, api_cache_csv: Path | None, use_api: bool = True):
        self.local_csv = local_csv
        self.api_cache_csv = api_cache_csv
        self.use_api = bool(use_api)
        self._local: Dict[str, float] = {}
        self._api_cache: Dict[str, float] = {}
        self._dirty = False
        self._debug_miss = 0
        self._load_local()
        self._load_api_cache()

    def _make_key(self, woj: str = "", powiat: str = "", gmina: str = "", miejscowosc: str = "",
                  dzielnica: str = "") -> str:
        w = _canon_admin(woj, "woj")
        p = _canon_admin(powiat, "pow")
        g = _canon_admin(gmina, "gmi")
        m = _canon_admin(miejscowosc, "mia")
        d = _canon_admin(dzielnica, "dzl")
        return "|".join([w, p, g, m, d])

    def _split_key(self, key: str) -> Tuple[str, str, str, str, str]:
        parts = (key.split("|") + ["", "", "", "", ""])[:5]
        return parts[0], parts[1], parts[2], parts[3], parts[4]

    def _candidate_keys(self, woj: str, powiat: str, gmina: str, miejscowosc: str, dzielnica: str) -> List[str]:
        # podstawowa hierarchia
        keys = [
            self._make_key(woj, powiat, gmina, miejscowosc, dzielnica),
            self._make_key(woj, powiat, gmina, miejscowosc, ""),
            self._make_key(woj, powiat, gmina, "", ""),
            self._make_key(woj, powiat, "", "", ""),
            self._make_key(woj, "", "", "", ""),
        ]

        # dodatkowe ĹcieĹźki gdy raport ma puste powiat/gmina, a csv ma wypeĹnione:
        keys += [
            self._make_key(woj, "", gmina, miejscowosc, dzielnica),
            self._make_key(woj, "", gmina, miejscowosc, ""),
            self._make_key(woj, "", gmina, "", ""),
            self._make_key(woj, powiat, "", miejscowosc, dzielnica),
            self._make_key(woj, powiat, "", miejscowosc, ""),
            self._make_key(woj, "", "", miejscowosc, dzielnica),
            self._make_key(woj, "", "", miejscowosc, ""),
        ]

        out, seen = [], set()
        for k in keys:
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(k)
        return out

    def _read_local_csv_any_sep(self, path: Path) -> pd.DataFrame:
        for sep in [";", ",", "\t"]:
            try:
                return pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8-sig", engine="python")
            except Exception:
                continue
        return pd.read_csv(path, sep=None, dtype=str, encoding="utf-8-sig", engine="python")

    def _load_local(self):
        if not self.local_csv:
            print("[PopulationResolver] local_csv=None (nie podano ĹcieĹźki).")
            return
        if not self.local_csv.exists():
            print(f"[PopulationResolver] local ludnosc.csv: NIE ISTNIEJE -> {self.local_csv}")
            return

        print(f"[PopulationResolver] WczytujÄ local ludnosc.csv -> {self.local_csv}")

        try:
            df = self._read_local_csv_any_sep(self.local_csv)
            print(f"[PopulationResolver] local rows={len(df)} cols={list(df.columns)}")

            col_woj = _find_col(df.columns, ["Wojewodztwo", "WojewĂłdztwo"])
            col_pow = _find_col(df.columns, ["Powiat"])
            col_gmi = _find_col(df.columns, ["Gmina"])
            col_mia = _find_col(df.columns, ["Miejscowosc", "MiejscowoĹÄ", "Miasto"])
            col_dzl = _find_col(df.columns, ["Dzielnica", "Osiedle"])
            col_pop = _find_col(df.columns,
                                ["ludnosc", "Ludnosc", "Liczba mieszkancow", "Liczba mieszkaĹcĂłw", "population"])

            print(
                f"[PopulationResolver] map cols: woj={col_woj} pow={col_pow} gmi={col_gmi} mia={col_mia} dzl={col_dzl} pop={col_pop}")

            if not col_pop:
                print("[PopulationResolver] local ludnosc.csv: brak kolumny ludnosc/population -> nie uĹźyjÄ pliku.")
                return

            loaded = 0
            for _, r in df.iterrows():
                pop_f = _to_float_maybe(r.get(col_pop))
                if pop_f is None:
                    continue

                woj = r.get(col_woj, "") if col_woj else ""
                powiat = r.get(col_pow, "") if col_pow else ""
                gmina = r.get(col_gmi, "") if col_gmi else ""
                miejsc = r.get(col_mia, "") if col_mia else ""
                dziel = r.get(col_dzl, "") if col_dzl else ""

                key = self._make_key(woj, powiat, gmina, miejsc, dziel)
                if key:
                    self._local[key] = float(pop_f)
                    loaded += 1

            print(f"[PopulationResolver] local loaded keys={loaded} (unikalne={len(self._local)})")

        except Exception as e:
            print(f"[PopulationResolver] Nie udaĹo siÄ wczytaÄ local ludnosc.csv: {e}")

    def _load_api_cache(self):
        if not self.api_cache_csv or not self.api_cache_csv.exists():
            return
        try:
            with self.api_cache_csv.open("r", encoding="utf-8-sig", newline="") as f:
                rd = csv.DictReader(f)
                for row in rd:
                    pop = _to_float_maybe(row.get("population", ""))
                    if pop is None:
                        continue
                    key = row.get("key") or self._make_key(
                        row.get("woj", ""), row.get("powiat", ""), row.get("gmina", ""),
                        row.get("miejscowosc", ""), row.get("dzielnica", "")
                    )
                    if key:
                        self._api_cache[key] = float(pop)
        except Exception as e:
            print(f"[PopulationResolver] Nie udaĹo siÄ wczytaÄ cache API: {e}")

    def _save_api_cache(self):
        if not self._dirty or not self.api_cache_csv:
            return
        try:
            self.api_cache_csv.parent.mkdir(parents=True, exist_ok=True)
            with self.api_cache_csv.open("w", encoding="utf-8-sig", newline="") as f:
                fieldnames = ["key", "woj", "powiat", "gmina", "miejscowosc", "dzielnica", "population"]
                wr = csv.DictWriter(f, fieldnames=fieldnames)
                wr.writeheader()
                for key, pop in self._api_cache.items():
                    parts = (key.split("|") + ["", "", "", "", ""])[:5]
                    woj, pow, gmi, mia, dzl = parts
                    wr.writerow({
                        "key": key,
                        "woj": woj,
                        "powiat": pow,
                        "gmina": gmi,
                        "miejscowosc": mia,
                        "dzielnica": dzl,
                        "population": pop,
                    })
            self._dirty = False
        except Exception as e:
            print(f"[PopulationResolver] Nie udaĹo siÄ zapisaÄ cache API: {e}")

    def _get_population_var_id(self) -> str | None:
        global _BDL_POP_VAR_ID

        if _BDL_POP_VAR_ID == _BDL_POP_VAR_ID_NOT_FOUND:
            return None
        if _BDL_POP_VAR_ID:
            return _BDL_POP_VAR_ID

        headers = _bdl_headers()
        if not headers:
            return None

        try:
            url = f"{BDL_BASE_URL}/variables"
            params = {"name": "ludnoĹÄ ogĂłĹem", "page-size": 50, "format": "json"}
            r = requests.get(url, headers=headers, params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                for v in data.get("results", []):
                    name = (v.get("name") or "").lower()
                    if "ludnoĹÄ ogĂłĹem" in name or "ludnosc ogolem" in name or "population total" in name:
                        _BDL_POP_VAR_ID = str(v.get("id"))
                        print(f"[PopulationResolver] Zmienna ludnoĹci: id={_BDL_POP_VAR_ID} ({name})")
                        return _BDL_POP_VAR_ID
        except Exception:
            pass

        print("[PopulationResolver] Nie znalazĹem zmiennej 'ludnoĹÄ ogĂłĹem' w BDL (cache).")
        _BDL_POP_VAR_ID = _BDL_POP_VAR_ID_NOT_FOUND
        return None

    def _fetch_population_from_api(self, woj: str, powiat: str, gmina: str, miejscowosc: str) -> Optional[float]:
        headers = _bdl_headers()
        if not headers:
            return None

        name_search = miejscowosc or gmina
        if not name_search:
            return None

        try:
            url_units = f"{BDL_BASE_URL}/units"
            params_units = {"name": name_search, "level": "6", "page-size": 50, "format": "json"}
            ru = requests.get(url_units, headers=headers, params=params_units, timeout=15)
            if ru.status_code != 200:
                return None
            ju = ru.json()
            units = ju.get("results", []) or []
            if not units:
                return None

            def score(u):
                nm = _plain(u.get("name") or "")
                sc = 0
                if _plain(name_search) == nm:
                    sc += 5
                elif _plain(name_search) in nm:
                    sc += 3
                if powiat and _plain(powiat) in nm:
                    sc += 1
                if woj and _plain(woj) in nm:
                    sc += 1
                return sc

            units.sort(key=score, reverse=True)
            unit_id = units[0].get("id")
            if not unit_id:
                return None
        except Exception:
            return None

        var_id = self._get_population_var_id()
        if not var_id:
            return None

        year = _pick_latest_year()
        try:
            url_data = f"{BDL_BASE_URL}/data/by-unit/{unit_id}"
            params_data = {"var-id": var_id, "year": str(year), "format": "json"}
            rd = requests.get(url_data, headers=headers, params=params_data, timeout=20)
            if rd.status_code != 200:
                return None

            jd = rd.json()
            results = jd.get("results") or []
            if not results:
                return None

            vals = results[0].get("values") or []
            for v in vals:
                raw = v[0] if isinstance(v, list) and len(v) >= 1 else v
                pop = _to_float_maybe(raw)
                if pop is not None:
                    return float(pop)
        except Exception:
            return None

        return None

    def _fallback_by_woj_mia(self, woj: str, miejscowosc: str, dzielnica: str) -> Optional[float]:
        """
        JeĹźeli peĹne klucze nie trafiajÄ (rĂłĹźnice w pow/gmi), sprĂłbuj:
        - dopasowaÄ po (woj + miejscowosc)
        - jeĹli dzielnica podana, preferuj rekordy z tÄ dzielnicÄ
        """
        woj_c = _canon_admin(woj, "woj")
        mia_c = _canon_admin(miejscowosc, "mia")
        dzl_c = _canon_admin(dzielnica, "dzl")

        if not woj_c or not mia_c:
            return None

        best_with_dzl = None
        best_any = None

        for key, pop in self._local.items():
            w, p, g, m, d = self._split_key(key)
            if w != woj_c or m != mia_c:
                continue
            if dzl_c and d == dzl_c:
                # preferuj dokĹadnÄ dzielnicÄ; jeĹli kilka, bierz najwiÄkszÄ (bezpiecznie)
                best_with_dzl = pop if (best_with_dzl is None or pop > best_with_dzl) else best_with_dzl
            else:
                best_any = pop if (best_any is None or pop > best_any) else best_any

        return best_with_dzl if best_with_dzl is not None else best_any

    def get_population(self, woj: str, powiat: str, gmina: str, miejscowosc: str, dzielnica: str) -> Optional[float]:
        # 1) local/cache: po kandydatach
        for key in self._candidate_keys(woj, powiat, gmina, miejscowosc, dzielnica):
            if key in self._local:
                return self._local[key]
            if key in self._api_cache:
                return self._api_cache[key]

        # 2) fallback: woj + miejscowosc (czÄsto raport ma inne pow/gmi niĹź csv)
        pop = self._fallback_by_woj_mia(woj, miejscowosc, dzielnica)
        if pop is not None:
            return float(pop)

        # 3) API
        if self.use_api:
            pop = self._fetch_population_from_api(woj, powiat, gmina, miejscowosc)
            if pop is not None:
                key4 = self._make_key(woj, powiat, gmina, miejscowosc, "")
                self._api_cache[key4] = float(pop)
                self._dirty = True
                self._save_api_cache()
                return float(pop)

        # maĹa diagnostyka: pokaĹź pierwsze 3 nietrafienia (Ĺźeby nie spamowaÄ)
        if self._debug_miss < 3:
            self._debug_miss += 1
            print("[PopulationResolver][MISS] szukaĹem dla:")
            print("  woj=", woj, "pow=", powiat, "gmi=", gmina, "mia=", miejscowosc, "dzl=", dzielnica)
            print("  canon key=", self._make_key(woj, powiat, gmina, miejscowosc, dzielnica))

        return None


# =========================
# Bezpieczny zapis XLSX (TYLKO arkusz 'raport')
# =========================


# =========================
# Core: przetwarzanie wiersza
# =========================

def _bucket_for_population(pop: float | None) -> tuple[float | None, float | None]:
    """Zwraca (low, high) dla progu ludnoĹci wg POP_MARGIN_RULES."""
    if pop is None:
        return (None, None)
    try:
        p = float(pop)
    except Exception:
        return (None, None)

    for low, high, _, _ in POP_MARGIN_RULES:
        if p >= low and (high is None or p < high):
            return (float(low), float(high) if high is not None else None)

    # fallback: ostatni prĂłg
    low, high, _, _ = POP_MARGIN_RULES[-1]
    return (float(low), float(high) if high is not None else None)


def _pop_in_bucket(pop: float | None, low: float | None, high: float | None) -> bool:
    if low is None and high is None:
        return True
    if pop is None:
        return False
    try:
        p = float(pop)
    except Exception:
        return False
    if high is None:
        return p >= float(low)
    return p >= float(low) and p < float(high)


@dataclass
class PolskaIndex:
    df: pd.DataFrame
    col_area: str
    col_price: str

    # kolumny adresowe w Polsce.xlsx (oryginalne)
    col_woj: str | None
    col_pow: str | None
    col_gmi: str | None
    col_mia: str | None
    col_dzl: str | None

    # kolumny kanoniczne
    c_woj: str | None
    c_pow: str | None
    c_gmi: str | None
    c_mia: str | None
    c_dzl: str | None

    # mapy miejscowoĹci -> przykĹadowa nazwa (Ĺźeby mĂłc pytaÄ PopulationResolver sensownym tekstem)
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]]
    by_powiat: Dict[Tuple[str, str], Dict[str, str]]
    by_woj: Dict[str, Dict[str, str]]


def build_polska_index(df_pl: pd.DataFrame, col_area_pl: str, col_price_pl: str) -> PolskaIndex:
    # kolumny administracyjne
    col_woj = _find_col(df_pl.columns, ["wojewodztwo", "wojewĂłdztwo", "woj"])
    col_pow = _find_col(df_pl.columns, ["powiat"])
    col_gmi = _find_col(df_pl.columns, ["gmina"])
    col_mia = _find_col(df_pl.columns, ["miejscowosc", "miejscowoĹÄ", "miasto"])
    col_dzl = _find_col(df_pl.columns, ["dzielnica", "osiedle"])

    # numeryczne metry/cena (jednorazowo â szybciej per wiersz raportu)
    if "_area_num" not in df_pl.columns:
        df_pl["_area_num"] = df_pl[col_area_pl].map(_to_float_maybe)
    if "_price_num" not in df_pl.columns:
        df_pl["_price_num"] = df_pl[col_price_pl].map(_to_float_maybe)

    # kanonizacja tekstĂłw do porĂłwnaĹ
    c_woj = c_pow = c_gmi = c_mia = c_dzl = None
    if col_woj:
        c_woj = "_woj_c"
        df_pl[c_woj] = df_pl[col_woj].map(lambda x: _canon_admin(x, "woj"))
    if col_pow:
        c_pow = "_pow_c"
        df_pl[c_pow] = df_pl[col_pow].map(lambda x: _canon_admin(x, "pow"))
    if col_gmi:
        c_gmi = "_gmi_c"
        df_pl[c_gmi] = df_pl[col_gmi].map(lambda x: _canon_admin(x, "gmi"))
    if col_mia:
        c_mia = "_mia_c"
        df_pl[c_mia] = df_pl[col_mia].map(lambda x: _canon_admin(x, "mia"))
    if col_dzl:
        c_dzl = "_dzl_c"
        df_pl[c_dzl] = df_pl[col_dzl].map(lambda x: _canon_admin(x, "dzl"))

    # mapy miejscowoĹci po gminie/powiecie/woj (na bazie tego co w ogĂłle jest w Polska.xlsx)
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    by_powiat: Dict[Tuple[str, str], Dict[str, str]] = {}
    by_woj: Dict[str, Dict[str, str]] = {}

    # brak miejscowoĹci -> brak indeksu (zostanÄ puste mapy, a fallback zadziaĹa tylko na miejscowoĹci)
    if c_woj and c_mia:
        # wojewĂłdztwo
        for w, gdf in df_pl.groupby(c_woj, dropna=False):
            if not w:
                continue
            mp: Dict[str, str] = {}
            if col_mia and c_mia:
                for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                    if not mia_c:
                        continue
                    # przykĹadowa oryginalna nazwa (z diakrytykami)
                    try:
                        ex = sub[col_mia].dropna().iloc[0]
                        mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                    except Exception:
                        mp[mia_c] = str(mia_c)
            by_woj[str(w)] = mp

        # powiat
        if c_pow:
            for (w, p), gdf in df_pl.groupby([c_woj, c_pow], dropna=False):
                if not w or not p:
                    continue
                mp: Dict[str, str] = {}
                if col_mia:
                    for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                        if not mia_c:
                            continue
                        try:
                            ex = sub[col_mia].dropna().iloc[0]
                            mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                        except Exception:
                            mp[mia_c] = str(mia_c)
                by_powiat[(str(w), str(p))] = mp

        # gmina
        if c_pow and c_gmi:
            for (w, p, g), gdf in df_pl.groupby([c_woj, c_pow, c_gmi], dropna=False):
                if not w or not p or not g:
                    continue
                mp: Dict[str, str] = {}
                if col_mia:
                    for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                        if not mia_c:
                            continue
                        try:
                            ex = sub[col_mia].dropna().iloc[0]
                            mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                        except Exception:
                            mp[mia_c] = str(mia_c)
                by_gmina[(str(w), str(p), str(g))] = mp

    return PolskaIndex(
        df=df_pl,
        col_area=col_area_pl,
        col_price=col_price_pl,
        col_woj=col_woj,
        col_pow=col_pow,
        col_gmi=col_gmi,
        col_mia=col_mia,
        col_dzl=col_dzl,
        c_woj=c_woj,
        c_pow=c_pow,
        c_gmi=c_gmi,
        c_mia=c_mia,
        c_dzl=c_dzl,
        by_gmina=by_gmina,
        by_powiat=by_powiat,
        by_woj=by_woj,
    )


def _mask_eq_canon(df: pd.DataFrame, canon_col: str | None, value_canon: str) -> pd.Series:
    if canon_col is None or not value_canon:
        return pd.Series(True, index=df.index)
    return df[canon_col].astype(str) == str(value_canon)


def _filter_miejscowosci_by_bucket(
        candidates: Dict[str, str],
        bucket_low: float | None,
        bucket_high: float | None,
        pop_resolver: PopulationResolver | None,
        woj_raw: str,
        pow_raw: str,
        gmi_raw: str,
        scope: str,
        pop_cache: Dict[Tuple[str, str], float | None],
) -> List[str]:
    """Zwraca listÄ kanonicznych nazw miejscowoĹci w zadanym progu ludnoĹci.
    JeĹźeli brakuje ludnoĹci dla wszystkich, zwrĂłci pustÄ listÄ (wtedy wyĹźej moĹźna zrobiÄ fallback)."""
    if not candidates:
        return []
    if bucket_low is None and bucket_high is None:
        return list(candidates.keys())

    out: List[str] = []
    for mia_c, mia_original in candidates.items():
        cache_key = (scope, mia_c)
        if cache_key in pop_cache:
            pop = pop_cache[cache_key]
        else:
            pop = None
            if pop_resolver is not None:
                pop = pop_resolver.get_population(woj_raw, pow_raw, gmi_raw, mia_original, "")
            pop_cache[cache_key] = pop
        if _pop_in_bucket(pop, bucket_low, bucket_high):
            out.append(mia_c)
    return out


# =========================
# Specjalne reguĹy dla Warszawy i jej aglomeracji
# =========================

def classify_location(mia_c: str, pow_c: str, woj_c: str) -> str:
    """Zwraca: 'warsaw_city' | 'warsaw_aglo' | 'normal'"""
    if (mia_c or '') == 'warszawa':
        return 'warsaw_city'
    # dynamiczne wczytanie listy â jednorazowo i cache w atrybucie funkcji
    aglo = getattr(classify_location, '_aglo_cache', None)
    if aglo is None:
        try:
            aglo = load_warsaw_agglomeration()
        except Exception:
            aglo = set(AGLO_WARSZAWA_DEFAULT)
        setattr(classify_location, '_aglo_cache', aglo)
    if (mia_c or '') in aglo and (woj_c or '') == 'mazowieckie':
        return 'warsaw_aglo'
    return 'normal'


def _select_comparables(
        pl: PolskaIndex,
        woj_c: str,
        pow_c: str,
        gmi_c: str,
        mia_c: str,
        dzl_c: str,
        woj_raw: str,
        pow_raw: str,
        gmi_raw: str,
        min_hits: int,
        bucket_low: float | None,
        bucket_high: float | None,
        pop_resolver: PopulationResolver | None,
        low_area: float,
        high_area: float,
        skip_city: bool = False) -> Tuple[pd.DataFrame, str]:
    df = pl.df
    # SCOPE ograniczajÄcy zasiÄg wyszukiwania wg reguĹ Warszawy
    policy = classify_location(mia_c, pow_c, woj_c)
    # Cache aglomeracji do maskowania
    _aglo_set = getattr(classify_location, "_aglo_cache", set(AGLO_WARSZAWA_DEFAULT))


def _scope_mask(base_mask: pd.Series) -> pd.Series:
    if policy == 'warsaw_city':
        m = base_mask.copy()
        if pl.c_mia:
            m &= (df[pl.c_mia] == 'warszawa')
        return m
    elif policy == 'warsaw_aglo':
        m = base_mask.copy()
        if pl.c_mia:
            m &= df[pl.c_mia].isin(list(_aglo_set))
            m &= (df[pl.c_mia] != 'warszawa')
        return m
    elif policy == 'voiv_capital':
        m = base_mask.copy()
        if pl.c_mia:
            m &= (df[pl.c_mia] == mia_c)
        return m
    else:
        return base_mask

        # _SCOPE_APPLY

    # progi minimalne per etap
    th_dzl = 5
    th_mia = 5
    th_gmi = 5
    th_pow = 10
    th_woj = 20

    # baza: zakres metrazu + musi miec cene
    base = (df["_area_num"].notna()) & (df["_area_num"] >= low_area) & (df["_area_num"] <= high_area) & df[
        "_price_num"].notna()

    pop_cache: Dict[Tuple[str, str], float | None] = {}

    def _take(mask: pd.Series, label: str) -> Tuple[pd.DataFrame, str]:
        sel = df[_scope_mask(mask)].copy()
        # tylko z cenÄ (juĹź jest), ale zabezpiecz
        sel = sel[sel["_price_num"].notna()].copy()
        return sel, label

    # 1) DZIELNICA (jeĹli podana)
    if not skip_city and dzl_c:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        mask &= _mask_eq_canon(df, pl.c_dzl, dzl_c)
        sel, label = _take(mask, "dzielnica")
        if len(sel.index) >= th_dzl:
            return sel, label
            return sel, label

    # 2) MIEJSCOWOĹÄ
    if not skip_city and mia_c:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        sel, label = _take(mask, "miejscowosc")
        if len(sel.index) >= th_mia:
            return sel, label
            return sel, label

    # 3) GMINA (miejscowoĹci w tym samym progu ludnoĹci)
    if gmi_c and pl.c_mia and pl.by_gmina:
        candidates = pl.by_gmina.get((woj_c, pow_c, gmi_c), {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw=pow_raw, gmi_raw=gmi_raw,
            scope="gmina", pop_cache=pop_cache
        )
        if not bucket_mias:
            # fallback: jak nie mamy ludnoĹci (lub brak danych), weĹş wszystkie miejscowoĹci z tej gminy
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= _mask_eq_canon(df, pl.c_pow, pow_c)
            mask &= _mask_eq_canon(df, pl.c_gmi, gmi_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "gmina(pop)")
            if len(sel.index) >= th_gmi:
                return sel, label
                return sel, label

    # 4) POWIAT (miejscowoĹci w tym samym progu ludnoĹci)
    if pow_c and pl.c_mia and pl.by_powiat:
        candidates = pl.by_powiat.get((woj_c, pow_c), {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw=pow_raw, gmi_raw="",
            scope="powiat", pop_cache=pop_cache
        )
        if not bucket_mias:
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= _mask_eq_canon(df, pl.c_pow, pow_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "powiat(pop)")
            if len(sel.index) >= th_pow:
                return sel, label
                return sel, label

    # 5) WOJEWĂDZTWO (miejscowoĹci w tym samym progu ludnoĹci)
    #    Specjalnie dla MAZOWIECKIEGO: zamiast przeszukiwaÄ caĹe mazowieckie, przeszukuj WOJ. SÄSIEDNIE (bez mazowieckiego)
    #    i zbierz WSZYSTKIE ogĹoszenia z tych wojewĂłdztw do wyliczeĹ.
    if woj_c and pl.c_mia and pl.by_woj:
        if woj_c == "mazowieckie":
            neighbors = [
                "lodzkie",
                "kujawsko pomorskie",
                "warminsko mazurskie",
                "podlaskie",
                "lubelskie",
                "swietokrzyskie",
            ]

            parts = []
            for w2 in neighbors:
                candidates = pl.by_woj.get(w2, {})
                if not candidates:
                    continue

                bucket_mias = _filter_miejscowosci_by_bucket(
                    candidates, bucket_low, bucket_high, pop_resolver,
                    woj_raw=w2, pow_raw="", gmi_raw="",
                    scope=f"woj_sas:{w2}", pop_cache=pop_cache
                )
                if not bucket_mias:
                    bucket_mias = list(candidates.keys())

                if not bucket_mias:
                    continue

                mask = base.copy()
                mask &= _mask_eq_canon(df, pl.c_woj, w2)
                mask &= df[pl.c_mia].isin(bucket_mias)
                sel_part, _ = _take(mask, f"woj_sas:{w2}")
                if not sel_part.empty:
                    parts.append(sel_part)

            if parts:
                sel = pd.concat(parts, axis=0, ignore_index=False)
                # usuĹ duplikaty (na wszelki wypadek)
                sel = sel.loc[~sel.index.duplicated(keep="first")].copy()
                # UWAGA: tu NIE przerywamy po pierwszych 5 â zbieramy peĹny zbiĂłr z sÄsiadĂłw do wyliczeĹ.
                if len(sel.index) >= th_woj:
                    return sel, "woj_sasiednie(pop)"
                # jeĹli < min_hits, i tak zwrĂłcimy to jako najlepszy szeroki zestaw (zamiast pustego)
                return sel, "woj_sasiednie(pop)_malo"

        # standard: dla innych wojewĂłdztw przeszukaj wĹasne woj.
        candidates = pl.by_woj.get(woj_c, {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw="", gmi_raw="",
            scope="woj", pop_cache=pop_cache
        )
        if not bucket_mias:
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "woj(pop)")
            if len(sel.index) >= th_woj:
                return sel, label
                return sel, label

    # jeĹli nadal < min_hits, zwrĂłÄ najlepsze co mamy (ostatni znaleziony) albo puste
    # sprĂłbuj chociaĹź na woj+miejscowoĹÄ bez progu (jeĹli progi odfiltrowaĹy wszystko)
    if woj_c and mia_c:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        sel, label = _take(mask, "miejscowosc(fallback)")
        if not sel.empty:
            return sel, label

    return df.iloc[0:0].copy(), "brak"


def _process_row(
        df_raport: pd.DataFrame,
        idx: int,
        pl: PolskaIndex,
        margin_m2_default: float,
        margin_pct_default: float,
        pop_resolver: PopulationResolver | None,
        min_hits: int = 5,
) -> None:
    row = df_raport.iloc[idx]

    kw_col = _find_col(df_raport.columns, ["Nr KW", "nr_kw", "nrksiegi", "nr ksiÄgi", "nr_ksiegi", "numer ksiÄgi"])
    kw_value = (str(row[kw_col]).strip() if (
                kw_col and pd.notna(row[kw_col]) and str(row[kw_col]).strip()) else f"WIERSZ_{idx + 1}")

    area_col = _find_col(df_raport.columns, ["Obszar", "metry", "powierzchnia"])
    area_val = _to_float_maybe(_trim_after_semicolon(row[area_col])) if area_col else None
    if area_val is None:
        print(f"[Automat] Wiersz {idx + 1}: brak obszaru â pomijam.")
        return

    def _get(cands):
        c = _find_col(df_raport.columns, cands)
        return _trim_after_semicolon(row[c]) if c else ""

    woj_r = _get(["WojewĂłdztwo", "Wojewodztwo", "wojewodztwo", "woj"])
    pow_r = _get(["Powiat"])
    gmi_r = _get(["Gmina"])
    mia_r = _get(["MiejscowoĹÄ", "Miejscowosc", "Miasto"])
    dzl_r = _get(["Dzielnica", "Osiedle"])

    woj_c = _canon_admin(woj_r, "woj")
    pow_c = _canon_admin(pow_r, "pow")
    gmi_c = _canon_admin(gmi_r, "gmi")
    mia_c = _canon_admin(mia_r, "mia")
    dzl_c = _canon_admin(dzl_r, "dzl")

    # =========================
    # TRYB STRICT (adres 100% wymagany)
    # =========================
    STRICT_MSG = "BRAK LUB NIEPEĹNY ADRESU â WPISZ ADRES MANUALNIE"

    mean_col = _find_col(df_raport.columns, ["Ĺrednia cena za m2 ( z bazy)", "Srednia cena za m2 ( z bazy)",
                                             "Ĺrednia cena za mÂ˛ ( z bazy)"])
    corr_col = _find_col(df_raport.columns, ["Ĺrednia skorygowana cena za m2", "Srednia skorygowana cena za m2"])
    val_col = _find_col(df_raport.columns,
                        ["Statystyczna wartoĹÄ nieruchomoĹci", "Statystyczna wartosc nieruchomosci"])

    if mean_col is None:
        mean_col = "Ĺrednia cena za m2 ( z bazy)"
        if mean_col not in df_raport.columns:
            df_raport[mean_col] = ""
    if corr_col is None:
        corr_col = "Ĺrednia skorygowana cena za m2"
        if corr_col not in df_raport.columns:
            df_raport[corr_col] = ""
    if val_col is None:
        val_col = "Statystyczna wartoĹÄ nieruchomoĹci"
        if val_col not in df_raport.columns:
            df_raport[val_col] = ""

        # ensure helper columns: hits & stage
        hits_col = _find_col(df_raport.columns, ['hits'])
        if hits_col is None:
            hits_col = 'hits'
            if hits_col not in df_raport.columns:
                df_raport[hits_col] = ''
        stage_col = _find_col(df_raport.columns, ['stage'])
        if stage_col is None:
            stage_col = 'stage'
            if stage_col not in df_raport.columns:
                df_raport[stage_col] = ''
        # place hits/stage between 'Czy udzialy?' and mean_col if possible
        try:
            col_czy = _find_col(df_raport.columns, ['Czy udzialy?', 'Czy udzialy', 'Czy udziaĹy?', 'Czy udziaĹy'])
            if col_czy is not None and mean_col in df_raport.columns:
                cols = list(df_raport.columns)
                for c in [hits_col, stage_col]:
                    if c in cols:
                        cols.remove(c)
                insert_at = cols.index(col_czy) + 1 if col_czy in cols else cols.index(mean_col)
                new_cols = cols[:insert_at] + [hits_col, stage_col] + cols[insert_at:]
                for i, c in enumerate(new_cols):
                    s = df_raport.pop(c)
                    df_raport.insert(i, c, s)
        except Exception:
            pass

    # Minimalne dane do pracy: woj + miejscowoĹÄ
    if not woj_c or not mia_c:
        df_raport.at[idx, mean_col] = STRICT_MSG
        df_raport.at[idx, corr_col] = STRICT_MSG
        df_raport.at[idx, val_col] = STRICT_MSG
        print(f"[Automat] {kw_value}: {STRICT_MSG} (woj='{woj_r}', mia='{mia_r}')")
        return

    # ludnoĹÄ + progi (m2 oraz %)
    pop_target = pop_resolver.get_population(woj_r, pow_r, gmi_r, mia_r, dzl_r) if pop_resolver else None
    bucket_low, bucket_high = _bucket_for_population(pop_target)

    if pop_target is None:
        margin_m2_row, margin_pct_row = float(margin_m2_default), float(margin_pct_default)
    else:
        margin_m2_row, margin_pct_row = rules_for_population(pop_target)

    delta = abs(float(margin_m2_row or 0.0))
    low_area, high_area = max(0.0, float(area_val) - delta), float(area_val) + delta

    df_sel, stage = _select_comparables(
        pl=pl,
        woj_c=woj_c,
        pow_c=pow_c,
        gmi_c=gmi_c,
        mia_c=mia_c,
        dzl_c=dzl_c,
        woj_raw=woj_r,
        pow_raw=pow_r,
        gmi_raw=gmi_r,
        min_hits=int(min_hits),
        bucket_low=bucket_low,
        bucket_high=bucket_high,
        pop_resolver=pop_resolver,
        low_area=low_area,
        high_area=high_area,
    )

    if df_sel.empty:
        msg = "BRAK OGLOSZEN W BAZIE DLA TEGO ZAKRESU"
        df_raport.at[idx, mean_col] = msg
        df_raport.at[idx, corr_col] = msg
        df_raport.at[idx, val_col] = msg
        try:
            df_raport.at[idx, hits_col] = 0
            df_raport.at[idx, stage_col] = str(stage)
        except Exception:
            pass
        print(f"[Automat] {kw_value}: {msg} (zakres {low_area:.2f}-{high_area:.2f} m2, stage={stage})")
        return

    # outliers â zawsze usuwamy wartoĹci brzegowe w wyliczeniach
    df_sel, _prices_arr = _filter_outliers_df(df_sel, "_price_num")
    prices = _prices_arr
    mean_price = float(np.nanmean(prices))
    try:
        df_raport.at[idx, hits_col] = int(len(prices))
    except Exception:
        df_raport.at[idx, hits_col] = len(prices)
    try:
        df_raport.at[idx, stage_col] = f"{stage} Âą{used_delta:.0f}"
    except Exception:
        pass

    # =========================
    # WYNIKI â ZAOKRÄGLENIE DO 2 MIEJSC
    # =========================
    mean_price_rounded = round(float(mean_price), 2)
    df_raport.at[idx, mean_col] = mean_price_rounded

    corrected_price = mean_price_rounded * (1.0 - float(margin_pct_row or 0.0) / 100.0)
    corrected_price_rounded = round(float(corrected_price), 2)
    df_raport.at[idx, corr_col] = corrected_price_rounded

    value = corrected_price_rounded * float(area_val)
    df_raport.at[idx, val_col] = round(float(value), 2)

    # log
    pop_txt = f"{int(pop_target):,}".replace(",", " ") if isinstance(pop_target, (int, float)) else "?"
    bucket_txt = f"{bucket_low}-{bucket_high if bucket_high is not None else 'â'}" if bucket_low is not None else "?"
    print(
        f"[Automat] {kw_value}: stage={stage} | hits={len(df_sel)} | pop={pop_txt} | bucket={bucket_txt} | mean={mean_price:.2f} | corr={corrected_price:.2f} | value={value:.2f}.")


# =========================
# MAIN

# =========================


# !/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

'''
automat.py â Wersja B (BDL + lokalna ludnoĹÄ + bezpieczny zapis arkusza 'raport')

FIX (17.12.2025+):
- ludnosc.csv jest wczytywane OK (logi), ale brak trafieĹ wynikaĹ z rĂłĹźnic w nazwach (pow./powiat, gmina miejska..., nawiasy)
- dodano kanonizacjÄ nazw jednostek (usuwa prefiksy/skrĂłtowce/nawiasy)
- dodano fallback dopasowania po (woj + miejscowosc) + preferencja dzielnicy
- zapis XLSX: openpyxl, tylko arkusz 'raport' (bez kasowania innych arkuszy)
'''

from pathlib import Path
import sys
import unicodedata
import csv
import os
import datetime
import re
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass

import pandas as pd
import numpy as np


def _filter_outliers_df(df, price_col: str):
    """Zawsze usuwa wartoĹci brzegowe z prĂłby cen (dla wyliczeĹ).

    Zasada:
    - n<=2: nie da siÄ sensownie przyciÄÄ -> zwracamy bez zmian
    - n=3..4: usuwamy min i max (zostajÄ wartoĹci Ĺrodkowe)
    - n>=5: filtr IQR (1.5*IQR); jeĹli da zbyt maĹo danych -> fallback do min/max
    """
    import numpy as _np

    if df is None or len(df.index) == 0:
        return df, _np.array([], dtype=float)

    prices_all = df[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan)
    valid = prices_all.dropna()
    n = int(len(valid))
    if n <= 2:
        return df, valid.to_numpy(dtype=float)

    # MaĹe prĂłby: obetnij skrajne wartoĹci (min/max)
    if n <= 4:
        order = valid.sort_values()
        keep_idx = order.iloc[1:-1].index
        df2 = df.loc[keep_idx].copy()
        prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()
        return df2, prices2.to_numpy(dtype=float)

    # IQR
    q1 = _np.nanpercentile(valid, 25)
    q3 = _np.nanpercentile(valid, 75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr

    mask = (prices_all >= lo) & (prices_all <= hi)
    df2 = df[mask].copy()
    prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()

    # JeĹli filtr IQR wyciÄĹ prawie wszystko, wrĂłÄ do prostego min/max
    if len(prices2) < 2:
        order = valid.sort_values()
        keep_idx = order.iloc[1:-1].index
        df2 = df.loc[keep_idx].copy()
        prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()

    return df2, prices2.to_numpy(dtype=float)

    q1 = _np.nanpercentile(valid, 25)
    q3 = _np.nanpercentile(valid, 75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr

    mask = (prices_all >= lo) & (prices_all <= hi)
    df2 = df[mask].copy()
    prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()

    if len(prices2) < 2:
        order = valid.sort_values()
        keep_idx = order.iloc[1:-1].index
        df2 = df.loc[keep_idx].copy()
        prices2 = df2[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan).dropna()

    return df2, prices2.to_numpy(dtype=float)


import importlib.util
import requests
from pathlib import Path


def import_local_automat():
    here = Path(__file__).resolve().parent
    p = here / "automat.py"
    spec = importlib.util.spec_from_file_location("automat", str(p))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# =========================
# Helpers
# =========================

def _norm(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "").replace("\xa0", "").replace("\t", "")


def _plain(x) -> str:
    """Bezpieczna normalizacja tekstu dla dowolnego typu (str/float/None/NaN)."""
    if x is None:
        return ""
    try:
        if isinstance(x, float) and np.isnan(x):
            return ""
    except Exception:
        pass

    s = str(x).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = " ".join(s.split())
    return s


def _strip_parentheses(s: str) -> str:
    # usuĹ nawiasy i zawartoĹÄ: "GdaĹsk (miasto)" -> "GdaĹsk"
    return re.sub(r"\([^)]*\)", " ", s).strip()


def _canon_admin(part: str, kind: str) -> str:
    """
    kind: woj/pow/gmi/mia/dzl
    Ujednolica teksty z raportu i csv:
    - usuwa nawiasy
    - usuwa znaki interpunkcyjne
    - usuwa sĹowa typu: powiat, pow., gmina, gm., woj., wojewĂłdztwo, itd.
    """
    s = _plain(part)
    if not s:
        return ""
    s = _strip_parentheses(s)

    # zamieĹ myĹlniki/slashe na spacje
    s = s.replace("-", " ").replace("/", " ")
    # wywal wszystko poza litery/cyfry/spacje
    s = re.sub(r"[^0-9a-z ]+", " ", s)
    s = " ".join(s.split())

    # tokeny do wywalenia
    drop_common = {
        "woj", "woj.", "wojewodztwo",
        "pow", "pow.", "powiat",
        "gmina", "gm", "gm.",
        "miasto", "m", "m.",
        "osiedle", "dzielnica",
        "miejska", "wiejska", "miejskowiejska", "miejsko", "wiejsko",
        "na", "prawach", "powiatu",
    }

    tokens = [t for t in s.split() if t not in drop_common]

    # czasem po usuniÄciu zostaje pusto â wtedy zostaw oryginalne (po plain)
    if not tokens:
        tokens = s.split()

    return " ".join(tokens).strip()


def _find_col(cols, candidates):
    norm_map = {_norm(c): c for c in cols}
    for cand in candidates:
        key = _norm(cand)
        if key in norm_map:
            return norm_map[key]
    for c in cols:
        if any(_norm(x) in _norm(c) for x in candidates):
            return c
    return None


def _trim_after_semicolon(val):
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    s = str(val)
    if ";" in s:
        s = s.split(";", 1)[0]
    return s.strip()


def _to_float_maybe(x):
    if x is None:
        return None
    try:
        if isinstance(x, float) and np.isnan(x):
            return None
    except Exception:
        pass

    s = str(x)
    for unit in ["mÂ˛", "m2", "zĹ/mÂ˛", "zĹ/m2", "zĹ"]:
        s = s.replace(unit, "")
    s = s.replace(" ", "").replace("\xa0", "").replace(",", ".")
    s = "".join(ch for ch in s if (ch.isdigit() or ch == "." or ch == "-"))
    try:
        return float(s) if s else None
    except Exception:
        return None


def _find_ludnosc_csv(baza_folder: Path, raport_path: Path, polska_path: Path) -> Path | None:
    """
    Szukamy *tylko* jednego ĹşrĂłdĹa ludnoĹci: pliku `ludnosc.csv`.

    Priorytet:
      1) folder raportu (tam gdzie jest plik raportu / Polska.xlsx wybierana w GUI)
      2) folder z `Polska.xlsx` (baza)
      3) `baza_folder` przekazany do automatu

    Dodatkowo: jeĹli trafimy na âstaryâ plik (np. ~2k wierszy), ignorujemy go,
    bo powinien byÄ peĹny (~100k rekordĂłw).
    """
    env = os.getenv("LUDNOSC_CSV_PATH")
    candidates: List[Path] = []
    if env:
        candidates.append(Path(env))
    candidates += [
        raport_path.parent / "ludnosc.csv",
        polska_path.parent / "ludnosc.csv",
        baza_folder / "ludnosc.csv",
    ]

    def _looks_full(p: Path) -> bool:
        try:
            with p.open("r", encoding="utf-8-sig", errors="ignore") as f:
                # -1 bo nagĹĂłwek
                n = sum(1 for _ in f) - 1
            return n >= 50000
        except Exception:
            return True  # nie blokuj w razie problemĂłw z odczytem

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                if _looks_full(p):
                    return p.resolve()
        except Exception:
            pass
    return None


# =========================
# Aglomeracja warszawska (miejscowoĹci bez Warszawy)
# =========================
# Uwaga: lista moĹźe byÄ nadpisana zewnÄtrznym plikiem (XLSX/CSV) â patrz load_warsaw_agglomeration().
AGLO_WARSZAWA_DEFAULT = {
    'piaseczno', 'konstancin jeziorna', 'gora kalwaria', 'lesznowola', 'prazmow', 'jozefow', 'otwock', 'celestynow',
    'karczew', 'kolbiel', 'wiazowna',
    'pruszkow', 'piastow', 'brwinow', 'michalowice', 'nadarzyn', 'raszyn',
    'blonie', 'izabelin', 'kampinos', 'leszno', 'stare babice', 'lomianki', 'ozarow mazowiecki',
    'marki', 'zabki', 'zielonka', 'wolomin', 'kobylka', 'radzymin', 'tluszcz', 'jadow', 'dabrowka', 'poswietne',
    'legionowo', 'jablonna', 'nieporet', 'serock', 'wieliszew', 'nowy dwor mazowiecki', 'czosnow', 'leoncin',
    'pomiechowek', 'zakroczym',
    'grodzisk mazowiecki', 'milanowek', 'podkowa lesna'
}

# Zestaw 18 stolic wojewodztw (kanonizowane, bez polskich znakow)
VOIVODE_CAPITALS = {
    'bialystok', 'bydgoszcz', 'torun', 'gdansk', 'gorzow wielkopolski', 'katowice', 'kielce', 'krakow',
    'lublin', 'lodz', 'olsztyn', 'opole', 'poznan', 'rzeszow', 'szczecin', 'warszawa', 'wroclaw', 'zielona gora'
}

from typing import Set
import pandas as _pd
import re as _re


def _canon_local(_s: str) -> str:
    s = str(_s or '').strip().lower()
    s = _re.sub(r"\(.*?\)", " ", s)
    s = s.replace('-', ' ').replace('/', ' ')
    s = ''.join(ch for ch in s if ch.isalnum() or ch.isspace())
    s = ' '.join(s.split())
    return s


def load_warsaw_agglomeration(hint_path: Path | None = None) -> Set[str]:
    # Wczytuje miejscowosci aglomeracji warszawskiej z pliku 'aglomeracja_warszawska.xlsx' (ten sam folder).
    # Jesli nie znajdzie - zwraca zestaw domyslny (AGLO_WARSZAWA_DEFAULT).
    candidates: list[Path] = []
    here = Path(__file__).resolve().parent
    candidates.append(here / 'aglomeracja_warszawska.xlsx')
    if hint_path:
        candidates.append(hint_path.parent / 'aglomeracja_warszawska.xlsx')
    import pandas as _pd
    try:
        for p in candidates:
            if not p.exists():
                continue
            xls = _pd.ExcelFile(p, engine='openpyxl')
            sheet = None
            for nm in xls.sheet_names:
                if 'reszta' in nm.lower() or 'aglo' in nm.lower():
                    sheet = nm
                    break
            if sheet is None:
                sheet = xls.sheet_names[0]
            df = _pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')
            cols = {str(c).lower(): c for c in df.columns}
            mia_col = None
            for key in ['miejsc', 'miejscowosc', 'miejscowoĹÄ', 'miasto']:
                if key in cols:
                    mia_col = cols[key]
                    break
            if mia_col is None:
                mia_col = list(df.columns)[-1]
            vals: set[str] = set()
            for v in df[mia_col].dropna().astype(str):
                c = _canon_local(v)
                if c and c != 'warszawa':
                    vals.add(c)
            if vals:
                return vals
    except Exception:
        pass
    return set(AGLO_WARSZAWA_DEFAULT)


# =========================
# Progi ludnoĹci (domyĹlne)
# (moĹźesz je zmieniaÄ w GUI: "Ustawienia progĂłw ludnoĹci")
# Format: (min_pop, max_pop | None, margin_m2, margin_pct)
# =========================

POP_MARGIN_RULES = [
    (0, 6000, 25.0, 15.0),
    (6000, 20000, 20.0, 15.0),
    (20000, 50000, 20.0, 15.0),
    (50000, 200000, 15.0, 15.0),
    (200000, None, 10.0, 15.0),
]


def rules_for_population(pop):
    if pop is None:
        return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])
    try:
        p = float(pop)
    except Exception:
        return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])

    for low, high, m2, pct in POP_MARGIN_RULES:
        if p >= low and (high is None or p < high):
            return float(m2), float(pct)
    return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])


def _eq_mask(df: pd.DataFrame, col_candidates, value: str) -> pd.Series:
    col = _find_col(df.columns, col_candidates)
    if col is None or not str(value).strip():
        return pd.Series(True, index=df.index)
    s = df[col].astype(str).str.strip().str.lower()
    v = str(value).strip().lower()
    return s == v


# =========================
# BDL / ludnoĹÄ
# =========================

BDL_BASE_URL = "https://bdl.stat.gov.pl/api/v1"
BDL_API_KEY_DEFAULT = "c804c054-f519-45b3-38f3-08de375a07dc"

_BDL_POP_VAR_ID: str | None = None
_BDL_POP_VAR_ID_NOT_FOUND = "__NOT_FOUND__"


def _bdl_headers() -> dict:
    api_key = os.getenv("BDL_API_KEY") or os.getenv("GUS_BDL_API_KEY") or BDL_API_KEY_DEFAULT
    if not api_key:
        return {}
    return {"X-ClientId": api_key, "Accept": "application/json"}


def _pick_latest_year():
    return datetime.date.today().year - 1


class PopulationResolver:
    def __init__(self, local_csv: Path | None, api_cache_csv: Path | None, use_api: bool = True):
        self.local_csv = local_csv
        self.api_cache_csv = api_cache_csv
        self.use_api = bool(use_api)
        self._local: Dict[str, float] = {}
        self._api_cache: Dict[str, float] = {}
        self._dirty = False
        self._debug_miss = 0
        self._load_local()
        self._load_api_cache()

    def _make_key(self, woj: str = "", powiat: str = "", gmina: str = "", miejscowosc: str = "",
                  dzielnica: str = "") -> str:
        w = _canon_admin(woj, "woj")
        p = _canon_admin(powiat, "pow")
        g = _canon_admin(gmina, "gmi")
        m = _canon_admin(miejscowosc, "mia")
        d = _canon_admin(dzielnica, "dzl")
        return "|".join([w, p, g, m, d])

    def _split_key(self, key: str) -> Tuple[str, str, str, str, str]:
        parts = (key.split("|") + ["", "", "", "", ""])[:5]
        return parts[0], parts[1], parts[2], parts[3], parts[4]

    def _candidate_keys(self, woj: str, powiat: str, gmina: str, miejscowosc: str, dzielnica: str) -> List[str]:
        # podstawowa hierarchia
        keys = [
            self._make_key(woj, powiat, gmina, miejscowosc, dzielnica),
            self._make_key(woj, powiat, gmina, miejscowosc, ""),
            self._make_key(woj, powiat, gmina, "", ""),
            self._make_key(woj, powiat, "", "", ""),
            self._make_key(woj, "", "", "", ""),
        ]

        # dodatkowe ĹcieĹźki gdy raport ma puste powiat/gmina, a csv ma wypeĹnione:
        keys += [
            self._make_key(woj, "", gmina, miejscowosc, dzielnica),
            self._make_key(woj, "", gmina, miejscowosc, ""),
            self._make_key(woj, "", gmina, "", ""),
            self._make_key(woj, powiat, "", miejscowosc, dzielnica),
            self._make_key(woj, powiat, "", miejscowosc, ""),
            self._make_key(woj, "", "", miejscowosc, dzielnica),
            self._make_key(woj, "", "", miejscowosc, ""),
        ]

        out, seen = [], set()
        for k in keys:
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(k)
        return out

    def _read_local_csv_any_sep(self, path: Path) -> pd.DataFrame:
        for sep in [";", ",", "\t"]:
            try:
                return pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8-sig", engine="python")
            except Exception:
                continue
        return pd.read_csv(path, sep=None, dtype=str, encoding="utf-8-sig", engine="python")

    def _load_local(self):
        if not self.local_csv:
            print("[PopulationResolver] local_csv=None (nie podano ĹcieĹźki).")
            return
        if not self.local_csv.exists():
            print(f"[PopulationResolver] local ludnosc.csv: NIE ISTNIEJE -> {self.local_csv}")
            return

        print(f"[PopulationResolver] WczytujÄ local ludnosc.csv -> {self.local_csv}")

        try:
            df = self._read_local_csv_any_sep(self.local_csv)
            print(f"[PopulationResolver] local rows={len(df)} cols={list(df.columns)}")

            col_woj = _find_col(df.columns, ["Wojewodztwo", "WojewĂłdztwo"])
            col_pow = _find_col(df.columns, ["Powiat"])
            col_gmi = _find_col(df.columns, ["Gmina"])
            col_mia = _find_col(df.columns, ["Miejscowosc", "MiejscowoĹÄ", "Miasto"])
            col_dzl = _find_col(df.columns, ["Dzielnica", "Osiedle"])
            col_pop = _find_col(df.columns,
                                ["ludnosc", "Ludnosc", "Liczba mieszkancow", "Liczba mieszkaĹcĂłw", "population"])

            print(
                f"[PopulationResolver] map cols: woj={col_woj} pow={col_pow} gmi={col_gmi} mia={col_mia} dzl={col_dzl} pop={col_pop}")

            if not col_pop:
                print("[PopulationResolver] local ludnosc.csv: brak kolumny ludnosc/population -> nie uĹźyjÄ pliku.")
                return

            loaded = 0
            for _, r in df.iterrows():
                pop_f = _to_float_maybe(r.get(col_pop))
                if pop_f is None:
                    continue

                woj = r.get(col_woj, "") if col_woj else ""
                powiat = r.get(col_pow, "") if col_pow else ""
                gmina = r.get(col_gmi, "") if col_gmi else ""
                miejsc = r.get(col_mia, "") if col_mia else ""
                dziel = r.get(col_dzl, "") if col_dzl else ""

                key = self._make_key(woj, powiat, gmina, miejsc, dziel)
                if key:
                    self._local[key] = float(pop_f)
                    loaded += 1

            print(f"[PopulationResolver] local loaded keys={loaded} (unikalne={len(self._local)})")

        except Exception as e:
            print(f"[PopulationResolver] Nie udaĹo siÄ wczytaÄ local ludnosc.csv: {e}")

    def _load_api_cache(self):
        if not self.api_cache_csv or not self.api_cache_csv.exists():
            return
        try:
            with self.api_cache_csv.open("r", encoding="utf-8-sig", newline="") as f:
                rd = csv.DictReader(f)
                for row in rd:
                    pop = _to_float_maybe(row.get("population", ""))
                    if pop is None:
                        continue
                    key = row.get("key") or self._make_key(
                        row.get("woj", ""), row.get("powiat", ""), row.get("gmina", ""),
                        row.get("miejscowosc", ""), row.get("dzielnica", "")
                    )
                    if key:
                        self._api_cache[key] = float(pop)
        except Exception as e:
            print(f"[PopulationResolver] Nie udaĹo siÄ wczytaÄ cache API: {e}")

    def _save_api_cache(self):
        if not self._dirty or not self.api_cache_csv:
            return
        try:
            self.api_cache_csv.parent.mkdir(parents=True, exist_ok=True)
            with self.api_cache_csv.open("w", encoding="utf-8-sig", newline="") as f:
                fieldnames = ["key", "woj", "powiat", "gmina", "miejscowosc", "dzielnica", "population"]
                wr = csv.DictWriter(f, fieldnames=fieldnames)
                wr.writeheader()
                for key, pop in self._api_cache.items():
                    parts = (key.split("|") + ["", "", "", "", ""])[:5]
                    woj, pow, gmi, mia, dzl = parts
                    wr.writerow({
                        "key": key,
                        "woj": woj,
                        "powiat": pow,
                        "gmina": gmi,
                        "miejscowosc": mia,
                        "dzielnica": dzl,
                        "population": pop,
                    })
            self._dirty = False
        except Exception as e:
            print(f"[PopulationResolver] Nie udaĹo siÄ zapisaÄ cache API: {e}")

    def _get_population_var_id(self) -> str | None:
        global _BDL_POP_VAR_ID

        if _BDL_POP_VAR_ID == _BDL_POP_VAR_ID_NOT_FOUND:
            return None
        if _BDL_POP_VAR_ID:
            return _BDL_POP_VAR_ID

        headers = _bdl_headers()
        if not headers:
            return None

        try:
            url = f"{BDL_BASE_URL}/variables"
            params = {"name": "ludnoĹÄ ogĂłĹem", "page-size": 50, "format": "json"}
            r = requests.get(url, headers=headers, params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                for v in data.get("results", []):
                    name = (v.get("name") or "").lower()
                    if "ludnoĹÄ ogĂłĹem" in name or "ludnosc ogolem" in name or "population total" in name:
                        _BDL_POP_VAR_ID = str(v.get("id"))
                        print(f"[PopulationResolver] Zmienna ludnoĹci: id={_BDL_POP_VAR_ID} ({name})")
                        return _BDL_POP_VAR_ID
        except Exception:
            pass

        print("[PopulationResolver] Nie znalazĹem zmiennej 'ludnoĹÄ ogĂłĹem' w BDL (cache).")
        _BDL_POP_VAR_ID = _BDL_POP_VAR_ID_NOT_FOUND
        return None

    def _fetch_population_from_api(self, woj: str, powiat: str, gmina: str, miejscowosc: str) -> Optional[float]:
        headers = _bdl_headers()
        if not headers:
            return None

        name_search = miejscowosc or gmina
        if not name_search:
            return None

        try:
            url_units = f"{BDL_BASE_URL}/units"
            params_units = {"name": name_search, "level": "6", "page-size": 50, "format": "json"}
            ru = requests.get(url_units, headers=headers, params=params_units, timeout=15)
            if ru.status_code != 200:
                return None
            ju = ru.json()
            units = ju.get("results", []) or []
            if not units:
                return None

            def score(u):
                nm = _plain(u.get("name") or "")
                sc = 0
                if _plain(name_search) == nm:
                    sc += 5
                elif _plain(name_search) in nm:
                    sc += 3
                if powiat and _plain(powiat) in nm:
                    sc += 1
                if woj and _plain(woj) in nm:
                    sc += 1
                return sc

            units.sort(key=score, reverse=True)
            unit_id = units[0].get("id")
            if not unit_id:
                return None
        except Exception:
            return None

        var_id = self._get_population_var_id()
        if not var_id:
            return None

        year = _pick_latest_year()
        try:
            url_data = f"{BDL_BASE_URL}/data/by-unit/{unit_id}"
            params_data = {"var-id": var_id, "year": str(year), "format": "json"}
            rd = requests.get(url_data, headers=headers, params=params_data, timeout=20)
            if rd.status_code != 200:
                return None

            jd = rd.json()
            results = jd.get("results") or []
            if not results:
                return None

            vals = results[0].get("values") or []
            for v in vals:
                raw = v[0] if isinstance(v, list) and len(v) >= 1 else v
                pop = _to_float_maybe(raw)
                if pop is not None:
                    return float(pop)
        except Exception:
            return None

        return None

    def _fallback_by_woj_mia(self, woj: str, miejscowosc: str, dzielnica: str) -> Optional[float]:
        """
        JeĹźeli peĹne klucze nie trafiajÄ (rĂłĹźnice w pow/gmi), sprĂłbuj:
        - dopasowaÄ po (woj + miejscowosc)
        - jeĹli dzielnica podana, preferuj rekordy z tÄ dzielnicÄ
        """
        woj_c = _canon_admin(woj, "woj")
        mia_c = _canon_admin(miejscowosc, "mia")
        dzl_c = _canon_admin(dzielnica, "dzl")

        if not woj_c or not mia_c:
            return None

        best_with_dzl = None
        best_any = None

        for key, pop in self._local.items():
            w, p, g, m, d = self._split_key(key)
            if w != woj_c or m != mia_c:
                continue
            if dzl_c and d == dzl_c:
                # preferuj dokĹadnÄ dzielnicÄ; jeĹli kilka, bierz najwiÄkszÄ (bezpiecznie)
                best_with_dzl = pop if (best_with_dzl is None or pop > best_with_dzl) else best_with_dzl
            else:
                best_any = pop if (best_any is None or pop > best_any) else best_any

        return best_with_dzl if best_with_dzl is not None else best_any

    def get_population(self, woj: str, powiat: str, gmina: str, miejscowosc: str, dzielnica: str) -> Optional[float]:
        # 1) local/cache: po kandydatach
        for key in self._candidate_keys(woj, powiat, gmina, miejscowosc, dzielnica):
            if key in self._local:
                return self._local[key]
            if key in self._api_cache:
                return self._api_cache[key]

        # 2) fallback: woj + miejscowosc (czÄsto raport ma inne pow/gmi niĹź csv)
        pop = self._fallback_by_woj_mia(woj, miejscowosc, dzielnica)
        if pop is not None:
            return float(pop)

        # 3) API
        if self.use_api:
            pop = self._fetch_population_from_api(woj, powiat, gmina, miejscowosc)
            if pop is not None:
                key4 = self._make_key(woj, powiat, gmina, miejscowosc, "")
                self._api_cache[key4] = float(pop)
                self._dirty = True
                self._save_api_cache()
                return float(pop)

        # maĹa diagnostyka: pokaĹź pierwsze 3 nietrafienia (Ĺźeby nie spamowaÄ)
        if self._debug_miss < 3:
            self._debug_miss += 1
            print("[PopulationResolver][MISS] szukaĹem dla:")
            print("  woj=", woj, "pow=", powiat, "gmi=", gmina, "mia=", miejscowosc, "dzl=", dzielnica)
            print("  canon key=", self._make_key(woj, powiat, gmina, miejscowosc, dzielnica))

        return None


# =========================
# Bezpieczny zapis XLSX (TYLKO arkusz 'raport')
# =========================


# =========================
# Core: przetwarzanie wiersza
# =========================

def _bucket_for_population(pop: float | None) -> tuple[float | None, float | None]:
    """Zwraca (low, high) dla progu ludnoĹci wg POP_MARGIN_RULES."""
    if pop is None:
        return (None, None)
    try:
        p = float(pop)
    except Exception:
        return (None, None)

    for low, high, _, _ in POP_MARGIN_RULES:
        if p >= low and (high is None or p < high):
            return (float(low), float(high) if high is not None else None)

    # fallback: ostatni prĂłg
    low, high, _, _ = POP_MARGIN_RULES[-1]
    return (float(low), float(high) if high is not None else None)


def _pop_in_bucket(pop: float | None, low: float | None, high: float | None) -> bool:
    if low is None and high is None:
        return True
    if pop is None:
        return False
    try:
        p = float(pop)
    except Exception:
        return False
    if high is None:
        return p >= float(low)
    return p >= float(low) and p < float(high)


@dataclass
class PolskaIndex:
    df: pd.DataFrame
    col_area: str
    col_price: str

    # kolumny adresowe w Polsce.xlsx (oryginalne)
    col_woj: str | None
    col_pow: str | None
    col_gmi: str | None
    col_mia: str | None
    col_dzl: str | None

    # kolumny kanoniczne
    c_woj: str | None
    c_pow: str | None
    c_gmi: str | None
    c_mia: str | None
    c_dzl: str | None

    # mapy miejscowoĹci -> przykĹadowa nazwa (Ĺźeby mĂłc pytaÄ PopulationResolver sensownym tekstem)
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]]
    by_powiat: Dict[Tuple[str, str], Dict[str, str]]
    by_woj: Dict[str, Dict[str, str]]


def build_polska_index(df_pl: pd.DataFrame, col_area_pl: str, col_price_pl: str) -> PolskaIndex:
    # kolumny administracyjne
    col_woj = _find_col(df_pl.columns, ["wojewodztwo", "wojewĂłdztwo", "woj"])
    col_pow = _find_col(df_pl.columns, ["powiat"])
    col_gmi = _find_col(df_pl.columns, ["gmina"])
    col_mia = _find_col(df_pl.columns, ["miejscowosc", "miejscowoĹÄ", "miasto"])
    col_dzl = _find_col(df_pl.columns, ["dzielnica", "osiedle"])

    # numeryczne metry/cena (jednorazowo â szybciej per wiersz raportu)
    if "_area_num" not in df_pl.columns:
        df_pl["_area_num"] = df_pl[col_area_pl].map(_to_float_maybe)
    if "_price_num" not in df_pl.columns:
        df_pl["_price_num"] = df_pl[col_price_pl].map(_to_float_maybe)

    # kanonizacja tekstĂłw do porĂłwnaĹ
    c_woj = c_pow = c_gmi = c_mia = c_dzl = None
    if col_woj:
        c_woj = "_woj_c"
        df_pl[c_woj] = df_pl[col_woj].map(lambda x: _canon_admin(x, "woj"))
    if col_pow:
        c_pow = "_pow_c"
        df_pl[c_pow] = df_pl[col_pow].map(lambda x: _canon_admin(x, "pow"))
    if col_gmi:
        c_gmi = "_gmi_c"
        df_pl[c_gmi] = df_pl[col_gmi].map(lambda x: _canon_admin(x, "gmi"))
    if col_mia:
        c_mia = "_mia_c"
        df_pl[c_mia] = df_pl[col_mia].map(lambda x: _canon_admin(x, "mia"))
    if col_dzl:
        c_dzl = "_dzl_c"
        df_pl[c_dzl] = df_pl[col_dzl].map(lambda x: _canon_admin(x, "dzl"))

    # mapy miejscowoĹci po gminie/powiecie/woj (na bazie tego co w ogĂłle jest w Polska.xlsx)
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    by_powiat: Dict[Tuple[str, str], Dict[str, str]] = {}
    by_woj: Dict[str, Dict[str, str]] = {}

    # brak miejscowoĹci -> brak indeksu (zostanÄ puste mapy, a fallback zadziaĹa tylko na miejscowoĹci)
    if c_woj and c_mia:
        # wojewĂłdztwo
        for w, gdf in df_pl.groupby(c_woj, dropna=False):
            if not w:
                continue
            mp: Dict[str, str] = {}
            if col_mia and c_mia:
                for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                    if not mia_c:
                        continue
                    # przykĹadowa oryginalna nazwa (z diakrytykami)
                    try:
                        ex = sub[col_mia].dropna().iloc[0]
                        mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                    except Exception:
                        mp[mia_c] = str(mia_c)
            by_woj[str(w)] = mp

        # powiat
        if c_pow:
            for (w, p), gdf in df_pl.groupby([c_woj, c_pow], dropna=False):
                if not w or not p:
                    continue
                mp: Dict[str, str] = {}
                if col_mia:
                    for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                        if not mia_c:
                            continue
                        try:
                            ex = sub[col_mia].dropna().iloc[0]
                            mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                        except Exception:
                            mp[mia_c] = str(mia_c)
                by_powiat[(str(w), str(p))] = mp

        # gmina
        if c_pow and c_gmi:
            for (w, p, g), gdf in df_pl.groupby([c_woj, c_pow, c_gmi], dropna=False):
                if not w or not p or not g:
                    continue
                mp: Dict[str, str] = {}
                if col_mia:
                    for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                        if not mia_c:
                            continue
                        try:
                            ex = sub[col_mia].dropna().iloc[0]
                            mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                        except Exception:
                            mp[mia_c] = str(mia_c)
                by_gmina[(str(w), str(p), str(g))] = mp

    return PolskaIndex(
        df=df_pl,
        col_area=col_area_pl,
        col_price=col_price_pl,
        col_woj=col_woj,
        col_pow=col_pow,
        col_gmi=col_gmi,
        col_mia=col_mia,
        col_dzl=col_dzl,
        c_woj=c_woj,
        c_pow=c_pow,
        c_gmi=c_gmi,
        c_mia=c_mia,
        c_dzl=c_dzl,
        by_gmina=by_gmina,
        by_powiat=by_powiat,
        by_woj=by_woj,
    )


def _mask_eq_canon(df: pd.DataFrame, canon_col: str | None, value_canon: str) -> pd.Series:
    if canon_col is None or not value_canon:
        return pd.Series(True, index=df.index)
    return df[canon_col].astype(str) == str(value_canon)


def _filter_miejscowosci_by_bucket(
        candidates: Dict[str, str],
        bucket_low: float | None,
        bucket_high: float | None,
        pop_resolver: PopulationResolver | None,
        woj_raw: str,
        pow_raw: str,
        gmi_raw: str,
        scope: str,
        pop_cache: Dict[Tuple[str, str], float | None],
) -> List[str]:
    """Zwraca listÄ kanonicznych nazw miejscowoĹci w zadanym progu ludnoĹci.
    JeĹźeli brakuje ludnoĹci dla wszystkich, zwrĂłci pustÄ listÄ (wtedy wyĹźej moĹźna zrobiÄ fallback)."""
    if not candidates:
        return []
    if bucket_low is None and bucket_high is None:
        return list(candidates.keys())

    out: List[str] = []
    for mia_c, mia_original in candidates.items():
        cache_key = (scope, mia_c)
        if cache_key in pop_cache:
            pop = pop_cache[cache_key]
        else:
            pop = None
            if pop_resolver is not None:
                pop = pop_resolver.get_population(woj_raw, pow_raw, gmi_raw, mia_original, "")
            pop_cache[cache_key] = pop
        if _pop_in_bucket(pop, bucket_low, bucket_high):
            out.append(mia_c)
    return out


# =========================
# Specjalne reguĹy dla Warszawy i jej aglomeracji
# =========================

def classify_location(mia_c: str, pow_c: str, woj_c: str) -> str:
    """Zwraca: 'warsaw_city' | 'warsaw_aglo' | 'normal'"""
    if (mia_c or '') == 'warszawa':
        return 'warsaw_city'
    # dynamiczne wczytanie listy â jednorazowo i cache w atrybucie funkcji
    aglo = getattr(classify_location, '_aglo_cache', None)
    if aglo is None:
        try:
            aglo = load_warsaw_agglomeration()
        except Exception:
            aglo = set(AGLO_WARSZAWA_DEFAULT)
        setattr(classify_location, '_aglo_cache', aglo)
    if (mia_c or '') in aglo and (woj_c or '') == 'mazowieckie':
        return 'warsaw_aglo'
    return 'normal'


def _select_comparables(
        pl: PolskaIndex,
        woj_c: str,
        pow_c: str,
        gmi_c: str,
        mia_c: str,
        dzl_c: str,
        woj_raw: str,
        pow_raw: str,
        gmi_raw: str,
        min_hits: int,
        bucket_low: float | None,
        bucket_high: float | None,
        pop_resolver: PopulationResolver | None,
        low_area: float,
        high_area: float,
        skip_city: bool = False) -> Tuple[pd.DataFrame, str]:
    df = pl.df
    # SCOPE ograniczajÄcy zasiÄg wyszukiwania wg reguĹ Warszawy
    policy = classify_location(mia_c, pow_c, woj_c)
    # Cache aglomeracji do maskowania
    _aglo_set = getattr(classify_location, "_aglo_cache", set(AGLO_WARSZAWA_DEFAULT))


def _scope_mask(base_mask: pd.Series) -> pd.Series:
    if policy == 'warsaw_city':
        m = base_mask.copy()
        if pl.c_mia:
            m &= (df[pl.c_mia] == 'warszawa')
        return m
    elif policy == 'warsaw_aglo':
        m = base_mask.copy()
        if pl.c_mia:
            m &= df[pl.c_mia].isin(list(_aglo_set))
            m &= (df[pl.c_mia] != 'warszawa')
        return m
    elif policy == 'voiv_capital':
        m = base_mask.copy()
        if pl.c_mia:
            m &= (df[pl.c_mia] == mia_c)
        return m
    else:
        return base_mask

        # _SCOPE_APPLY

    # progi minimalne per etap
    th_dzl = 5
    th_mia = 5
    th_gmi = 5
    th_pow = 10
    th_woj = 20

    # baza: zakres metrazu + musi miec cene
    base = (df["_area_num"].notna()) & (df["_area_num"] >= low_area) & (df["_area_num"] <= high_area) & df[
        "_price_num"].notna()

    pop_cache: Dict[Tuple[str, str], float | None] = {}

    def _take(mask: pd.Series, label: str) -> Tuple[pd.DataFrame, str]:
        sel = df[_scope_mask(mask)].copy()
        # tylko z cenÄ (juĹź jest), ale zabezpiecz
        sel = sel[sel["_price_num"].notna()].copy()
        return sel, label

    # 1) DZIELNICA (jeĹli podana)
    if not skip_city and dzl_c:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        mask &= _mask_eq_canon(df, pl.c_dzl, dzl_c)
        sel, label = _take(mask, "dzielnica")
        if len(sel.index) >= th_dzl:
            return sel, label
            return sel, label

    # 2) MIEJSCOWOĹÄ
    if not skip_city and mia_c:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        sel, label = _take(mask, "miejscowosc")
        if len(sel.index) >= th_mia:
            return sel, label
            return sel, label

    # 3) GMINA (miejscowoĹci w tym samym progu ludnoĹci)
    if gmi_c and pl.c_mia and pl.by_gmina:
        candidates = pl.by_gmina.get((woj_c, pow_c, gmi_c), {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw=pow_raw, gmi_raw=gmi_raw,
            scope="gmina", pop_cache=pop_cache
        )
        if not bucket_mias:
            # fallback: jak nie mamy ludnoĹci (lub brak danych), weĹş wszystkie miejscowoĹci z tej gminy
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= _mask_eq_canon(df, pl.c_pow, pow_c)
            mask &= _mask_eq_canon(df, pl.c_gmi, gmi_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "gmina(pop)")
            if len(sel.index) >= th_gmi:
                return sel, label
                return sel, label

    # 4) POWIAT (miejscowoĹci w tym samym progu ludnoĹci)
    if pow_c and pl.c_mia and pl.by_powiat:
        candidates = pl.by_powiat.get((woj_c, pow_c), {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw=pow_raw, gmi_raw="",
            scope="powiat", pop_cache=pop_cache
        )
        if not bucket_mias:
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= _mask_eq_canon(df, pl.c_pow, pow_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "powiat(pop)")
            if len(sel.index) >= th_pow:
                return sel, label
                return sel, label

    # 5) WOJEWĂDZTWO (miejscowoĹci w tym samym progu ludnoĹci)
    #    Specjalnie dla MAZOWIECKIEGO: zamiast przeszukiwaÄ caĹe mazowieckie, przeszukuj WOJ. SÄSIEDNIE (bez mazowieckiego)
    #    i zbierz WSZYSTKIE ogĹoszenia z tych wojewĂłdztw do wyliczeĹ.
    if woj_c and pl.c_mia and pl.by_woj:
        if woj_c == "mazowieckie":
            neighbors = [
                "lodzkie",
                "kujawsko pomorskie",
                "warminsko mazurskie",
                "podlaskie",
                "lubelskie",
                "swietokrzyskie",
            ]

            parts = []
            for w2 in neighbors:
                candidates = pl.by_woj.get(w2, {})
                if not candidates:
                    continue

                bucket_mias = _filter_miejscowosci_by_bucket(
                    candidates, bucket_low, bucket_high, pop_resolver,
                    woj_raw=w2, pow_raw="", gmi_raw="",
                    scope=f"woj_sas:{w2}", pop_cache=pop_cache
                )
                if not bucket_mias:
                    bucket_mias = list(candidates.keys())

                if not bucket_mias:
                    continue

                mask = base.copy()
                mask &= _mask_eq_canon(df, pl.c_woj, w2)
                mask &= df[pl.c_mia].isin(bucket_mias)
                sel_part, _ = _take(mask, f"woj_sas:{w2}")
                if not sel_part.empty:
                    parts.append(sel_part)

            if parts:
                sel = pd.concat(parts, axis=0, ignore_index=False)
                # usuĹ duplikaty (na wszelki wypadek)
                sel = sel.loc[~sel.index.duplicated(keep="first")].copy()
                # UWAGA: tu NIE przerywamy po pierwszych 5 â zbieramy peĹny zbiĂłr z sÄsiadĂłw do wyliczeĹ.
                if len(sel.index) >= th_woj:
                    return sel, "woj_sasiednie(pop)"
                # jeĹli < min_hits, i tak zwrĂłcimy to jako najlepszy szeroki zestaw (zamiast pustego)
                return sel, "woj_sasiednie(pop)_malo"

        # standard: dla innych wojewĂłdztw przeszukaj wĹasne woj.
        candidates = pl.by_woj.get(woj_c, {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw="", gmi_raw="",
            scope="woj", pop_cache=pop_cache
        )
        if not bucket_mias:
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "woj(pop)")
            if len(sel.index) >= th_woj:
                return sel, label
                return sel, label

    # jeĹli nadal < min_hits, zwrĂłÄ najlepsze co mamy (ostatni znaleziony) albo puste
    # sprĂłbuj chociaĹź na woj+miejscowoĹÄ bez progu (jeĹli progi odfiltrowaĹy wszystko)
    if woj_c and mia_c:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        sel, label = _take(mask, "miejscowosc(fallback)")
        if not sel.empty:
            return sel, label

    return df.iloc[0:0].copy(), "brak"


def _process_row(
        df_raport: pd.DataFrame,
        idx: int,
        pl: PolskaIndex,
        margin_m2_default: float,
        margin_pct_default: float,
        pop_resolver: PopulationResolver | None,
        min_hits: int = 5,
) -> None:
    row = df_raport.iloc[idx]

    kw_col = _find_col(df_raport.columns, ["Nr KW", "nr_kw", "nrksiegi", "nr ksiÄgi", "nr_ksiegi", "numer ksiÄgi"])
    kw_value = (str(row[kw_col]).strip() if (
                kw_col and pd.notna(row[kw_col]) and str(row[kw_col]).strip()) else f"WIERSZ_{idx + 1}")

    area_col = _find_col(df_raport.columns, ["Obszar", "metry", "powierzchnia"])
    area_val = _to_float_maybe(_trim_after_semicolon(row[area_col])) if area_col else None
    if area_val is None:
        print(f"[Automat] Wiersz {idx + 1}: brak obszaru â pomijam.")
        return

    def _get(cands):
        c = _find_col(df_raport.columns, cands)
        return _trim_after_semicolon(row[c]) if c else ""

    woj_r = _get(["WojewĂłdztwo", "Wojewodztwo", "wojewodztwo", "woj"])
    pow_r = _get(["Powiat"])
    gmi_r = _get(["Gmina"])
    mia_r = _get(["MiejscowoĹÄ", "Miejscowosc", "Miasto"])
    dzl_r = _get(["Dzielnica", "Osiedle"])

    woj_c = _canon_admin(woj_r, "woj")
    pow_c = _canon_admin(pow_r, "pow")
    gmi_c = _canon_admin(gmi_r, "gmi")
    mia_c = _canon_admin(mia_r, "mia")
    dzl_c = _canon_admin(dzl_r, "dzl")

    # =========================
    # TRYB STRICT (adres 100% wymagany)
    # =========================
    STRICT_MSG = "BRAK LUB NIEPEĹNY ADRESU â WPISZ ADRES MANUALNIE"

    mean_col = _find_col(df_raport.columns, ["Ĺrednia cena za m2 ( z bazy)", "Srednia cena za m2 ( z bazy)",
                                             "Ĺrednia cena za mÂ˛ ( z bazy)"])
    corr_col = _find_col(df_raport.columns, ["Ĺrednia skorygowana cena za m2", "Srednia skorygowana cena za m2"])
    val_col = _find_col(df_raport.columns,
                        ["Statystyczna wartoĹÄ nieruchomoĹci", "Statystyczna wartosc nieruchomosci"])

    if mean_col is None:
        mean_col = "Ĺrednia cena za m2 ( z bazy)"
        if mean_col not in df_raport.columns:
            df_raport[mean_col] = ""
    if corr_col is None:
        corr_col = "Ĺrednia skorygowana cena za m2"
        if corr_col not in df_raport.columns:
            df_raport[corr_col] = ""
    if val_col is None:
        val_col = "Statystyczna wartoĹÄ nieruchomoĹci"
        if val_col not in df_raport.columns:
            df_raport[val_col] = ""

        # ensure helper columns: hits & stage
        hits_col = _find_col(df_raport.columns, ['hits'])
        if hits_col is None:
            hits_col = 'hits'
            if hits_col not in df_raport.columns:
                df_raport[hits_col] = ''
        stage_col = _find_col(df_raport.columns, ['stage'])
        if stage_col is None:
            stage_col = 'stage'
            if stage_col not in df_raport.columns:
                df_raport[stage_col] = ''
        # place hits/stage between 'Czy udzialy?' and mean_col if possible
        try:
            col_czy = _find_col(df_raport.columns, ['Czy udzialy?', 'Czy udzialy', 'Czy udziaĹy?', 'Czy udziaĹy'])
            if col_czy is not None and mean_col in df_raport.columns:
                cols = list(df_raport.columns)
                for c in [hits_col, stage_col]:
                    if c in cols:
                        cols.remove(c)
                insert_at = cols.index(col_czy) + 1 if col_czy in cols else cols.index(mean_col)
                new_cols = cols[:insert_at] + [hits_col, stage_col] + cols[insert_at:]
                for i, c in enumerate(new_cols):
                    s = df_raport.pop(c)
                    df_raport.insert(i, c, s)
        except Exception:
            pass

    # Minimalne dane do pracy: woj + miejscowoĹÄ
    if not woj_c or not mia_c:
        df_raport.at[idx, mean_col] = STRICT_MSG
        df_raport.at[idx, corr_col] = STRICT_MSG
        df_raport.at[idx, val_col] = STRICT_MSG
        print(f"[Automat] {kw_value}: {STRICT_MSG} (woj='{woj_r}', mia='{mia_r}')")
        return

    # ludnoĹÄ + progi (m2 oraz %)
    pop_target = pop_resolver.get_population(woj_r, pow_r, gmi_r, mia_r, dzl_r) if pop_resolver else None
    bucket_low, bucket_high = _bucket_for_population(pop_target)

    if pop_target is None:
        margin_m2_row, margin_pct_row = float(margin_m2_default), float(margin_pct_default)
    else:
        margin_m2_row, margin_pct_row = rules_for_population(pop_target)

    delta = abs(float(margin_m2_row or 0.0))
    low_area, high_area = max(0.0, float(area_val) - delta), float(area_val) + delta

    df_sel, stage = _select_comparables(
        pl=pl,
        woj_c=woj_c,
        pow_c=pow_c,
        gmi_c=gmi_c,
        mia_c=mia_c,
        dzl_c=dzl_c,
        woj_raw=woj_r,
        pow_raw=pow_r,
        gmi_raw=gmi_r,
        min_hits=int(min_hits),
        bucket_low=bucket_low,
        bucket_high=bucket_high,
        pop_resolver=pop_resolver,
        low_area=low_area,
        high_area=high_area,
    )

    if df_sel.empty:
        msg = "BRAK OGLOSZEN W BAZIE DLA TEGO ZAKRESU"
        df_raport.at[idx, mean_col] = msg
        df_raport.at[idx, corr_col] = msg
        df_raport.at[idx, val_col] = msg
        try:
            df_raport.at[idx, hits_col] = 0
            df_raport.at[idx, stage_col] = str(stage)
        except Exception:
            pass
        print(f"[Automat] {kw_value}: {msg} (zakres {low_area:.2f}-{high_area:.2f} m2, stage={stage})")
        return

    # outliers â zawsze usuwamy wartoĹci brzegowe w wyliczeniach
    df_sel, _prices_arr = _filter_outliers_df(df_sel, "_price_num")
    prices = _prices_arr
    mean_price = float(np.nanmean(prices))
    try:
        df_raport.at[idx, hits_col] = int(len(prices))
    except Exception:
        df_raport.at[idx, hits_col] = len(prices)
    try:
        df_raport.at[idx, stage_col] = f"{stage} Âą{used_delta:.0f}"
    except Exception:
        pass

    # =========================
    # WYNIKI â ZAOKRÄGLENIE DO 2 MIEJSC
    # =========================
    mean_price_rounded = round(float(mean_price), 2)ann
    df_raport.at[idx, mean_col] = mean_price_rounded

    corrected_price = mean_price_rounded * (1.0 - float(margin_pct_row or 0.0) / 100.0)
    corrected_price_rounded = round(float(corrected_price), 2)
    df_raport.at[idx, corr_col] = corrected_price_rounded

    value = corrected_price_rounded * float(area_val)
    df_raport.at[idx, val_col] = round(float(value), 2)

    # log
    pop_txt = f"{int(pop_target):,}".replace(",", " ") if isinstance(pop_target, (int, float)) else "?"
    bucket_txt = f"{bucket_low}-{bucket_high if bucket_high is not None else 'â'}" if bucket_low is not None else "?"
    print(
        f"[Automat] {kw_value}: stage={stage} | hits={len(df_sel)} | pop={pop_txt} | bucket={bucket_txt} | mean={mean_price:.2f} | corr={corrected_price:.2f} | value={value:.2f}.")

# =========================
# MAIN

# =========================



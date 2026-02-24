#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

'''
automat.py — Wersja B (BDL + lokalna ludność + bezpieczny zapis arkusza 'raport')

FIX (17.12.2025+):
- ludnosc.csv jest wczytywane OK (logi), ale brak trafień wynikał z różnic w nazwach (pow./powiat, gmina miejska..., nawiasy)
- dodano kanonizację nazw jednostek (usuwa prefiksy/skrótowce/nawiasy)
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
    """Zawsze usuwa wartości brzegowe z próby cen (dla wyliczeń).

    Zasada:
    - n<=2: nie da się sensownie przyciąć -> zwracamy bez zmian
    - n=3..4: usuwamy min i max (zostają wartości środkowe)
    - n>=5: filtr IQR (1.5*IQR); jeśli da zbyt mało danych -> fallback do min/max
    """
    import numpy as _np

    if df is None or len(df.index) == 0:
        return df, _np.array([], dtype=float)

    prices_all = df[price_col].astype(float).replace([_np.inf, -_np.inf], _np.nan)
    valid = prices_all.dropna()
    n = int(len(valid))
    if n <= 2:
        return df, valid.to_numpy(dtype=float)

    # Małe próby: obetnij skrajne wartości (min/max)
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

    # Jeśli filtr IQR wyciął prawie wszystko, wróć do prostego min/max
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
    # usuń nawiasy i zawartość: "Gdańsk (miasto)" -> "Gdańsk"
    return re.sub(r"\([^)]*\)", " ", s).strip()

def _canon_admin(part: str, kind: str) -> str:
    """
    kind: woj/pow/gmi/mia/dzl
    Ujednolica teksty z raportu i csv:
    - usuwa nawiasy
    - usuwa znaki interpunkcyjne
    - usuwa słowa typu: powiat, pow., gmina, gm., woj., województwo, itd.
    """
    s = _plain(part)
    if not s:
        return ""
    s = _strip_parentheses(s)

    # zamień myślniki/slashe na spacje
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

    # czasem po usunięciu zostaje pusto – wtedy zostaw oryginalne (po plain)
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
    for unit in ["m²", "m2", "zł/m²", "zł/m2", "zł"]:
        s = s.replace(unit, "")
    s = s.replace(" ", "").replace("\xa0", "").replace(",", ".")
    s = "".join(ch for ch in s if (ch.isdigit() or ch == "." or ch == "-"))
    try:
        return float(s) if s else None
    except Exception:
        return None


def _find_ludnosc_csv(baza_folder: Path, raport_path: Path, polska_path: Path) -> Path | None:
    """
    Szukamy *tylko* jednego źródła ludności: pliku `ludnosc.csv`.

    Priorytet:
      1) folder raportu (tam gdzie jest plik raportu / Polska.xlsx wybierana w GUI)
      2) folder z `Polska.xlsx` (baza)
      3) `baza_folder` przekazany do automatu

    Dodatkowo: jeśli trafimy na „stary” plik (np. ~2k wierszy), ignorujemy go,
    bo powinien być pełny (~100k rekordów).
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
                # -1 bo nagłówek
                n = sum(1 for _ in f) - 1
            return n >= 50000
        except Exception:
            return True  # nie blokuj w razie problemów z odczytem

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                if _looks_full(p):
                    return p.resolve()
        except Exception:
            pass
    return None




# =========================
# Aglomeracja warszawska (miejscowości bez Warszawy)
# =========================
# Uwaga: lista może być nadpisana zewnętrznym plikiem (XLSX/CSV) — patrz load_warsaw_agglomeration().
AGLO_WARSZAWA_DEFAULT = {
    'piaseczno','konstancin jeziorna','gora kalwaria','lesznowola','prazmow','jozefow','otwock','celestynow','karczew','kolbiel','wiazowna',
    'pruszkow','piastow','brwinow','michalowice','nadarzyn','raszyn',
    'blonie','izabelin','kampinos','leszno','stare babice','lomianki','ozarow mazowiecki',
    'marki','zabki','zielonka','wolomin','kobylka','radzymin','tluszcz','jadow','dabrowka','poswietne',
    'legionowo','jablonna','nieporet','serock','wieliszew','nowy dwor mazowiecki','czosnow','leoncin','pomiechowek','zakroczym',
    'grodzisk mazowiecki','milanowek','podkowa lesna'
}

# Zestaw 18 stolic wojewodztw (kanonizowane, bez polskich znakow)
VOIVODE_CAPITALS = {
    'bialystok','bydgoszcz','torun','gdansk','gorzow wielkopolski','katowice','kielce','krakow',
    'lublin','lodz','olsztyn','opole','poznan','rzeszow','szczecin','warszawa','wroclaw','zielona gora'
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
            for key in ['miejsc', 'miejscowosc', 'miejscowość', 'miasto']:
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
# Progi ludności (domyślne)
# (możesz je zmieniać w GUI: "Ustawienia progów ludności")
# Format: (min_pop, max_pop | None, margin_m2, margin_pct)
# =========================

POP_MARGIN_RULES = [
    (0,       6000,   25.0, 15.0),
    (6000,   20000,   20.0, 15.0),
    (20000,  50000,   20.0, 15.0),
    (50000, 200000,   15.0, 15.0),
    (200000,   None,  10.0, 15.0),
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
# BDL / ludność
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

    def _make_key(self, woj: str = "", powiat: str = "", gmina: str = "", miejscowosc: str = "", dzielnica: str = "") -> str:
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

        # dodatkowe ścieżki gdy raport ma puste powiat/gmina, a csv ma wypełnione:
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
            print("[PopulationResolver] local_csv=None (nie podano ścieżki).")
            return
        if not self.local_csv.exists():
            print(f"[PopulationResolver] local ludnosc.csv: NIE ISTNIEJE -> {self.local_csv}")
            return

        print(f"[PopulationResolver] Wczytuję local ludnosc.csv -> {self.local_csv}")

        try:
            df = self._read_local_csv_any_sep(self.local_csv)
            print(f"[PopulationResolver] local rows={len(df)} cols={list(df.columns)}")

            col_woj = _find_col(df.columns, ["Wojewodztwo", "Województwo"])
            col_pow = _find_col(df.columns, ["Powiat"])
            col_gmi = _find_col(df.columns, ["Gmina"])
            col_mia = _find_col(df.columns, ["Miejscowosc", "Miejscowość", "Miasto"])
            col_dzl = _find_col(df.columns, ["Dzielnica", "Osiedle"])
            col_pop = _find_col(df.columns, ["ludnosc", "Ludnosc", "Liczba mieszkancow", "Liczba mieszkańców", "population"])

            print(f"[PopulationResolver] map cols: woj={col_woj} pow={col_pow} gmi={col_gmi} mia={col_mia} dzl={col_dzl} pop={col_pop}")

            if not col_pop:
                print("[PopulationResolver] local ludnosc.csv: brak kolumny ludnosc/population -> nie użyję pliku.")
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
            print(f"[PopulationResolver] Nie udało się wczytać local ludnosc.csv: {e}")

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
            print(f"[PopulationResolver] Nie udało się wczytać cache API: {e}")

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
            print(f"[PopulationResolver] Nie udało się zapisać cache API: {e}")

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
            params = {"name": "ludność ogółem", "page-size": 50, "format": "json"}
            r = requests.get(url, headers=headers, params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                for v in data.get("results", []):
                    name = (v.get("name") or "").lower()
                    if "ludność ogółem" in name or "ludnosc ogolem" in name or "population total" in name:
                        _BDL_POP_VAR_ID = str(v.get("id"))
                        print(f"[PopulationResolver] Zmienna ludności: id={_BDL_POP_VAR_ID} ({name})")
                        return _BDL_POP_VAR_ID
        except Exception:
            pass

        print("[PopulationResolver] Nie znalazłem zmiennej 'ludność ogółem' w BDL (cache).")
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
        Jeżeli pełne klucze nie trafiają (różnice w pow/gmi), spróbuj:
        - dopasować po (woj + miejscowosc)
        - jeśli dzielnica podana, preferuj rekordy z tą dzielnicą
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
                # preferuj dokładną dzielnicę; jeśli kilka, bierz największą (bezpiecznie)
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

        # 2) fallback: woj + miejscowosc (często raport ma inne pow/gmi niż csv)
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

        # mała diagnostyka: pokaż pierwsze 3 nietrafienia (żeby nie spamować)
        if self._debug_miss < 3:
            self._debug_miss += 1
            print("[PopulationResolver][MISS] szukałem dla:")
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
    """Zwraca (low, high) dla progu ludności wg POP_MARGIN_RULES."""
    if pop is None:
        return (None, None)
    try:
        p = float(pop)
    except Exception:
        return (None, None)

    for low, high, _, _ in POP_MARGIN_RULES:
        if p >= low and (high is None or p < high):
            return (float(low), float(high) if high is not None else None)

    # fallback: ostatni próg
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

    # mapy miejscowości -> przykładowa nazwa (żeby móc pytać PopulationResolver sensownym tekstem)
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]]
    by_powiat: Dict[Tuple[str, str], Dict[str, str]]
    by_woj: Dict[str, Dict[str, str]]

def build_polska_index(df_pl: pd.DataFrame, col_area_pl: str, col_price_pl: str) -> PolskaIndex:
    # kolumny administracyjne
    col_woj = _find_col(df_pl.columns, ["wojewodztwo", "województwo", "woj"])
    col_pow = _find_col(df_pl.columns, ["powiat"])
    col_gmi = _find_col(df_pl.columns, ["gmina"])
    col_mia = _find_col(df_pl.columns, ["miejscowosc", "miejscowość", "miasto"])
    col_dzl = _find_col(df_pl.columns, ["dzielnica", "osiedle"])

    # numeryczne metry/cena (jednorazowo – szybciej per wiersz raportu)
    if "_area_num" not in df_pl.columns:
        df_pl["_area_num"] = df_pl[col_area_pl].map(_to_float_maybe)
    if "_price_num" not in df_pl.columns:
        df_pl["_price_num"] = df_pl[col_price_pl].map(_to_float_maybe)

    # kanonizacja tekstów do porównań
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

    # mapy miejscowości po gminie/powiecie/woj (na bazie tego co w ogóle jest w Polska.xlsx)
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    by_powiat: Dict[Tuple[str, str], Dict[str, str]] = {}
    by_woj: Dict[str, Dict[str, str]] = {}

    # brak miejscowości -> brak indeksu (zostaną puste mapy, a fallback zadziała tylko na miejscowości)
    if c_woj and c_mia:
        # województwo
        for w, gdf in df_pl.groupby(c_woj, dropna=False):
            if not w:
                continue
            mp: Dict[str, str] = {}
            if col_mia and c_mia:
                for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                    if not mia_c:
                        continue
                    # przykładowa oryginalna nazwa (z diakrytykami)
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
    """Zwraca listę kanonicznych nazw miejscowości w zadanym progu ludności.
    Jeżeli brakuje ludności dla wszystkich, zwróci pustą listę (wtedy wyżej można zrobić fallback)."""
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
# Specjalne reguły dla Warszawy i jej aglomeracji
# =========================



def classify_location(mia_c: str, pow_c: str, woj_c: str) -> str:
    """Zwraca: 'warsaw_city' | 'warsaw_aglo' | 'voiv_capital' | 'normal'"""
    if (mia_c or '') == 'warszawa':
        return 'warsaw_city'
    # stolice województw -> tylko miasto
    if (mia_c or '') in VOIVODE_CAPITALS:
        return 'voiv_capital'
    # dynamiczne wczytanie listy aglomeracji – cache w atrybucie funkcji
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
# =========================
# Core: przetwarzanie wiersza raportu (BRAKUJĄCE w wersji pliku)
# + "skakanie" pomiarem brzegowym: 3/6/9/... aż do max z progu ludności
# =========================

VALUE_COLS = [
    "Średnia cena za m2 ( z bazy)",
    "Średnia skorygowana cena za m2",
    "Statystyczna wartość nieruchomości",
]

HITS_COL = "hits"
STAGE_COL = "stage"

# kolumna "kotwica" – dokładnie między nią a VALUE_COLS[0] wstawiamy hits+stage
ANCHOR_COL = "Czy udziały?"

MISSING_ADDR_TEXT = "brak adresu"
NO_OFFERS_TEXT = "brak ogłoszeń w zakresie"
MISSING_AREA_TEXT = "brak metrażu"

def ensure_report_columns(df_report: pd.DataFrame) -> None:
    """Zapewnia, że raport ma kolumny wynikowe + diagnostyczne (hits/stage).
    Nie zmienia kolejności kolumn – do tego służy reorder_report_columns().
    """
    if df_report is None:
        return
    for c in [HITS_COL, STAGE_COL, *VALUE_COLS]:
        if c not in df_report.columns:
            df_report[c] = np.nan

def reorder_report_columns(df_report: pd.DataFrame) -> pd.DataFrame:
    """Zwraca DF z uporządkowanymi kolumnami:
    ... 'Czy udziały?' , hits , stage , 'Średnia cena za m2 ( z bazy)' , ...
    """
    if df_report is None or df_report.empty:
        return df_report
    ensure_report_columns(df_report)

    cols = list(df_report.columns)

    # nic nie robimy, jeśli nie mamy kotwicy – tylko upewniamy się, że kolumny istnieją
    if ANCHOR_COL not in cols:
        return df_report

    desired = [HITS_COL, STAGE_COL, VALUE_COLS[0]]

    # usuń desired z bieżącej listy
    for c in desired:
        if c in cols:
            cols.remove(c)

    pos = cols.index(ANCHOR_COL) + 1
    cols[pos:pos] = desired

    # pozostałe VALUE_COLS (1..2) zostawiamy tam gdzie były lub dopisz na koniec
    for c in VALUE_COLS[1:]:
        if c not in cols:
            cols.append(c)

    return df_report.reindex(columns=cols)

# kompatybilność wstecz (stare wywołania)
def _ensure_value_cols(df_report: pd.DataFrame) -> None:
    ensure_report_columns(df_report)

def _iter_m2_steps(max_margin: float, step: float = 3.0) -> List[float]:
    """Zwraca listę: 3,6,9,...,max_margin (zawsze zawiera max_margin jako ostatni krok)."""
    try:
        max_m = float(max_margin)
    except Exception:
        max_m = 0.0
    try:
        st = float(step)
    except Exception:
        st = 3.0
    if st <= 0:
        st = 3.0
    if max_m <= 0:
        return []
    steps: List[float] = []
    k = 1
    while k * st < max_m - 1e-9:
        steps.append(round(k * st, 6))
        k += 1
        if k > 10_000:
            break
    steps.append(float(max_m))
    return steps

def _select_candidates_dynamic_margin(
    pl: PolskaIndex,
    base_mask: pd.Series,
    area_target: float,
    max_margin_m2: float,
    step_m2: float,
    prefer_mask: pd.Series | None,
    min_hits: int,
) -> tuple[pd.DataFrame, float, bool]:
    """Zwraca (df_kandydatów, użyty_margin, czy_użyto_preferencji).

    Implementuje eskalację pomiarem brzegowym: 3/6/9/... aż do max_margin_m2.
    Jeśli prefer_mask jest podany (np. dzielnica), to najpierw próbujemy z nim,
    a dopiero potem bez niego.

    Jeśli nie znajdzie nic nawet przy maksymalnym marginesie, zwraca pusty DF,
    a jako used_margin zwraca max_margin_m2 (żeby w stage było widać, że przeszedł cały zakres).
    """
    best_df = pl.df.iloc[0:0].copy()
    best_margin = 0.0
    best_used_pref = False

    steps = _iter_m2_steps(max_margin_m2, step_m2)
    last_tried = float(steps[-1]) if steps else 0.0

    for m in steps:
        # z preferencją (dzielnica)
        if prefer_mask is not None:
            df1 = pl.df[base_mask & prefer_mask].copy()
            df1 = df1[df1["_price_num"].notna()].copy()
            df1 = df1[df1["_area_num"].notna()].copy()
            df1 = df1[(df1["_area_num"] - area_target).abs() <= m].copy()
            if len(df1.index) > len(best_df.index):
                best_df, best_margin, best_used_pref = df1, m, True
            if len(df1.index) >= min_hits:
                return df1, m, True

        # bez preferencji
        df2 = pl.df[base_mask].copy()
        df2 = df2[df2["_price_num"].notna()].copy()
        df2 = df2[df2["_area_num"].notna()].copy()
        df2 = df2[(df2["_area_num"] - area_target).abs() <= m].copy()
        if len(df2.index) > len(best_df.index):
            best_df, best_margin, best_used_pref = df2, m, False
        if len(df2.index) >= min_hits:
            return df2, m, False

    # jeżeli nie było żadnej poprawy (0 trafień wszędzie), pokaż w stage max margines
    if best_df is None or len(best_df.index) == 0:
        best_margin = last_tried
    return best_df, best_margin, best_used_pref

def _build_stage_masks(
    pl: PolskaIndex,
    woj_c: str,
    pow_c: str,
    gmi_c: str,
    mia_c: str,
    loc_class: str,
) -> list[tuple[str, pd.Series]]:
    """Buduje listę (stage_name, maska) w kolejności od najbardziej do najmniej restrykcyjnej."""
    df_pl = pl.df
    masks: list[tuple[str, pd.Series]] = []

    base = pd.Series(True, index=df_pl.index)
    if pl.c_woj and woj_c:
        base &= _mask_eq_canon(df_pl, pl.c_woj, woj_c)

    # Warszawa / stolice województw: tylko miasto (w obrębie woj)
    if loc_class in ("voiv_capital", "warsaw_city"):
        if pl.c_mia and mia_c:
            masks.append(("miasto", base & _mask_eq_canon(df_pl, pl.c_mia, mia_c)))
        else:
            masks.append(("woj", base))
        return masks

    # Aglomeracja warszawska (bez Warszawy): miasta z listy aglo w obrębie Mazowieckiego
    if loc_class == "warsaw_aglo":
        aglo = getattr(classify_location, "_aglo_cache", None)
        if aglo is None:
            try:
                aglo = load_warsaw_agglomeration()
            except Exception:
                aglo = set(AGLO_WARSZAWA_DEFAULT)
            setattr(classify_location, "_aglo_cache", aglo)

        if pl.c_mia:
            col = df_pl[pl.c_mia].astype(str).str.strip().str.lower()
            masks.append(("aglo", base & col.isin(list(aglo))))
        else:
            masks.append(("woj", base))
        return masks

    # NORMAL: próbujemy pełniej, potem coraz luźniej.
    # 1) woj + pow + gmi + miasto
    if pl.c_pow and pow_c and pl.c_gmi and gmi_c and pl.c_mia and mia_c:
        masks.append(("pow+gmi+miasto", base
                      & _mask_eq_canon(df_pl, pl.c_pow, pow_c)
                      & _mask_eq_canon(df_pl, pl.c_gmi, gmi_c)
                      & _mask_eq_canon(df_pl, pl.c_mia, mia_c)))

    # 2) woj + gmi + miasto
    if pl.c_gmi and gmi_c and pl.c_mia and mia_c:
        masks.append(("gmi+miasto", base
                      & _mask_eq_canon(df_pl, pl.c_gmi, gmi_c)
                      & _mask_eq_canon(df_pl, pl.c_mia, mia_c)))

    # 3) woj + pow + miasto
    if pl.c_pow and pow_c and pl.c_mia and mia_c:
        masks.append(("pow+miasto", base
                      & _mask_eq_canon(df_pl, pl.c_pow, pow_c)
                      & _mask_eq_canon(df_pl, pl.c_mia, mia_c)))

    # 4) woj + miasto
    if pl.c_mia and mia_c:
        masks.append(("miasto", base & _mask_eq_canon(df_pl, pl.c_mia, mia_c)))

    # 5) woj + gmi
    if pl.c_gmi and gmi_c:
        masks.append(("gmi", base & _mask_eq_canon(df_pl, pl.c_gmi, gmi_c)))

    # 6) woj + pow
    if pl.c_pow and pow_c:
        masks.append(("pow", base & _mask_eq_canon(df_pl, pl.c_pow, pow_c)))

    # 7) tylko woj
    masks.append(("woj", base))

    # usuń duplikaty masek po nazwie i treści (na wypadek braków kolumn)
    uniq: list[tuple[str, pd.Series]] = []
    seen = set()
    for name, m in masks:
        key = (name, int(m.sum()))
        if key in seen:
            continue
        seen.add(key)
        uniq.append((name, m))
    return uniq

def _process_row(
    df_raport: pd.DataFrame,
    idx: int,
    pl: PolskaIndex,
    margin_m2_default: float = 15.0,
    margin_pct_default: float = 15.0,
    pop_resolver: PopulationResolver | None = None,
    *,
    min_hits: int = 5,
    step_m2: float = 3.0,
) -> None:
    """Przetwarza pojedynczy wiersz raportu i wpisuje wyniki.

    - Jeśli brak danych adresowych (Województwo/Powiat/Gmina/Miejscowość) -> 'brak adresu'
      w kolumnie 'Średnia cena za m2 ( z bazy)' oraz hits=0.
    - Jeśli po przejściu całej logiki nie ma min_hits ogłoszeń -> 'brak ogłoszeń w zakresie'
      w kolumnie 'Średnia cena za m2 ( z bazy)' + hits + stage.
    - Eskalacja pomiarem brzegowym: 3/6/9/... aż do max z progu ludności.
    - Stage pokazuje etap filtrowania terytorialnego, margines m² i czy użyto dzielnicy.
    """
    if df_raport is None or idx < 0 or idx >= len(df_raport.index):
        return

    ensure_report_columns(df_raport)
    row = df_raport.iloc[idx]
    row_key = df_raport.index[idx]

    def _set_status(avg_text: str, hits: int, stage: str) -> None:
        df_raport.at[row_key, HITS_COL] = int(hits) if hits is not None else 0
        df_raport.at[row_key, STAGE_COL] = stage
        df_raport.at[row_key, VALUE_COLS[0]] = avg_text
        df_raport.at[row_key, VALUE_COLS[1]] = np.nan
        df_raport.at[row_key, VALUE_COLS[2]] = np.nan

    def _set_values(avg: float, corrected: float, value: float, hits: int, stage: str) -> None:
        df_raport.at[row_key, HITS_COL] = int(hits) if hits is not None else 0
        df_raport.at[row_key, STAGE_COL] = stage
        df_raport.at[row_key, VALUE_COLS[0]] = avg
        df_raport.at[row_key, VALUE_COLS[1]] = corrected
        df_raport.at[row_key, VALUE_COLS[2]] = value

    # --- kolumny raportu ---
    col_woj = _find_col(df_raport.columns, ["Województwo", "Wojewodztwo", "woj"])
    col_pow = _find_col(df_raport.columns, ["Powiat"])
    col_gmi = _find_col(df_raport.columns, ["Gmina"])
    col_mia = _find_col(df_raport.columns, ["Miejscowość", "Miejscowosc", "Miasto", "miejsc"])
    col_dzl = _find_col(df_raport.columns, ["Dzielnica", "Osiedle"])
    col_area = _find_col(df_raport.columns, ["Obszar", "metry", "powierzchnia"])

    # Brak kolumn adresowych -> brak adresu (wymóg)
    if not (col_woj and col_pow and col_gmi and col_mia):
        _set_status(MISSING_ADDR_TEXT, 0, "brak_kolumn_adresu")
        return

    woj_raw = _trim_after_semicolon(row[col_woj]) if col_woj else ""
    pow_raw = _trim_after_semicolon(row[col_pow]) if col_pow else ""
    gmi_raw = _trim_after_semicolon(row[col_gmi]) if col_gmi else ""
    mia_raw = _trim_after_semicolon(row[col_mia]) if col_mia else ""
    dzl_raw = _trim_after_semicolon(row[col_dzl]) if col_dzl else ""
    area_val = _to_float_maybe(row[col_area]) if col_area else None

    if not woj_raw or not pow_raw or not gmi_raw or not mia_raw:
        _set_status(MISSING_ADDR_TEXT, 0, "brak_adresu")
        return

    if area_val is None:
        _set_status(MISSING_AREA_TEXT, 0, "brak_metrazu")
        return

    # --- kanonizacja ---
    woj_c = _canon_admin(woj_raw, "woj")
    pow_c = _canon_admin(pow_raw, "pow")
    gmi_c = _canon_admin(gmi_raw, "gmi")
    mia_c = _canon_admin(mia_raw, "mia")
    dzl_c = _canon_admin(dzl_raw, "dzl")

    # --- ludność -> progi (max margin + % negocjacyjny) ---
    pop = None
    if pop_resolver is not None:
        pop = pop_resolver.get_population(woj_raw, pow_raw, gmi_raw, mia_raw, dzl_raw)

    margin_m2, margin_pct = rules_for_population(pop)

    if not (isinstance(margin_m2, (int, float)) and float(margin_m2) > 0):
        margin_m2 = float(margin_m2_default)
    if not isinstance(margin_pct, (int, float)):
        margin_pct = float(margin_pct_default)

    loc_class = classify_location(mia_c, pow_c, woj_c)

    # preferencja dzielnicy
    df_pl = pl.df
    prefer_mask = None
    if dzl_c and pl.c_dzl:
        prefer_mask = _mask_eq_canon(df_pl, pl.c_dzl, dzl_c)

    # --- Etapy filtrowania terytorialnego + skakanie pomiarem brzegowym ---
    stage_masks = _build_stage_masks(pl, woj_c, pow_c, gmi_c, mia_c, loc_class)

    best_df = pl.df.iloc[0:0].copy()
    best_hits = 0
    best_used_m = float(margin_m2)
    best_used_dzl = False
    best_stage_name = stage_masks[-1][0] if stage_masks else "woj"

    for stage_name, base_mask in stage_masks:
        cand_df, used_m, used_dzl = _select_candidates_dynamic_margin(
            pl=pl,
            base_mask=base_mask,
            area_target=float(area_val),
            max_margin_m2=float(margin_m2),
            step_m2=float(step_m2),
            prefer_mask=prefer_mask,
            min_hits=int(min_hits),
        )
        cand_n = int(len(cand_df.index)) if cand_df is not None else 0

        if cand_n > best_hits:
            best_df, best_hits = cand_df, cand_n
            best_used_m, best_used_dzl = float(used_m), bool(used_dzl)
            best_stage_name = stage_name

        if cand_n >= int(min_hits):
            best_df, best_hits = cand_df, cand_n
            best_used_m, best_used_dzl = float(used_m), bool(used_dzl)
            best_stage_name = stage_name
            break

    cand_df = best_df
    cand_n = int(len(cand_df.index)) if cand_df is not None else 0
    stage_base = f"{loc_class}:{best_stage_name}|m={float(best_used_m):g}|{'dzielnica' if best_used_dzl else 'bez_dzielnicy'}"

    if cand_df is None or cand_n < int(min_hits):
        _set_status(NO_OFFERS_TEXT, cand_n, f"{stage_base}|hits<{int(min_hits)}")
        return

    # Zawsze usuwamy wartości brzegowe przed średnią
    cand_df2, prices = _filter_outliers_df(cand_df, "_price_num")
    avg = float(np.mean(prices)) if prices is not None and len(prices) else None
    if avg is None:
        _set_status(NO_OFFERS_TEXT, cand_n, f"{stage_base}|no_price")
        return

    corrected = float(avg) * (1.0 - (float(margin_pct) / 100.0))
    value = corrected * float(area_val)
    _set_values(avg, corrected, value, cand_n, stage_base)

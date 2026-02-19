#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
manual.py — ręczne liczenie (GUI: Selektor CSV) wg nowego algorytmu.

Nowy algorytm doboru porównywalnych ogłoszeń (min_hits=5):
1) dzielnica
2) miejscowość
3) gmina (miejscowości w tym samym progu ludności)
4) powiat (miejscowości w tym samym progu ludności)
5) województwo (miejscowości w tym samym progu ludności)
   - wyjątek: MAZOWIECKIE -> zamiast mazowieckiego, szukaj w sąsiednich woj. (bez mazowieckiego)
     i ZBIERAJ WSZYSTKIE ogłoszenia z tych województw do wyliczeń.

Źródła:
- baza ogłoszeń: Polska.xlsx (w folderze bazowym)
- progi ludności: ludnosc.csv (w folderze bazowym; format jak u Ciebie)

Ta funkcja jest wywoływana z selektor_csv.py po kliknięciu "Oblicz i zapisz ten wiersz".
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import unicodedata
import re

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



# =========================
# Błędy
# =========================

class ManualUserError(RuntimeError):
    """Błąd, który użytkownik może naprawić (brak plików/kolumn, itp.)."""


# =========================
# Progi ludności (jak w automacie)
# (min_pop, max_pop, margin_m2, margin_pct)
# =========================

POP_MARGIN_RULES: List[Tuple[int, Optional[int], float, float]] = [
    (0,      6000,    25.0, 15.0),
    (6000,   20000,   20.0, 15.0),
    (20000,  50000,   20.0, 15.0),
    (50000,  200000,  15.0, 15.0),
    (200000, None,    10.0, 15.0),
]


def rules_for_population(pop: float) -> Tuple[float, float]:
    """Zwraca (margin_m2, margin_pct) wg POP_MARGIN_RULES."""
    try:
        p = float(pop)
    except Exception:
        return 15.0, 15.0
    for lo, hi, m2, pct in POP_MARGIN_RULES:
        if hi is None:
            if p >= lo:
                return float(m2), float(pct)
        else:
            if lo <= p < hi:
                return float(m2), float(pct)
    return 15.0, 15.0


def bucket_for_population(pop: Optional[float]) -> Tuple[Optional[int], Optional[int]]:
    """Zwraca (bucket_low, bucket_high) wg POP_MARGIN_RULES."""
    if pop is None:
        return None, None
    try:
        p = float(pop)
    except Exception:
        return None, None
    for lo, hi, _, _ in POP_MARGIN_RULES:
        if hi is None:
            if p >= lo:
                return int(lo), None
        else:
            if lo <= p < hi:
                return int(lo), int(hi)
    return None, None


# =========================
# Tekst / kolumny / liczby
# =========================

def _norm(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "").replace("\xa0", "").replace("\t", "")


def _plain(x) -> str:
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
    return re.sub(r"\([^)]*\)", " ", s).strip()


def _canon_admin(part: str, kind: str) -> str:
    """
    kind: woj/pow/gmi/mia/dzl
    Ujednolica teksty z raportu i csv:
    - usuwa nawiasy
    - usuwa interpunkcję
    - usuwa tokeny typu: powiat, gmina, woj., itd.
    """
    s = _plain(part)
    if not s:
        return ""
    s = _strip_parentheses(s)

    s = s.replace("-", " ").replace("/", " ")
    s = re.sub(r"[^0-9a-z ]+", " ", s)
    s = " ".join(s.split())

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
        s = s.split(";", 1)[0].strip()
    return s


def _to_float_maybe(x):
    if x is None:
        return None
    try:
        if pd.isna(x):
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


# =========================
# Ludność (lokalny CSV)
# =========================

def _find_ludnosc_csv(base_dir: Path) -> Optional[Path]:
    candidates = [
        base_dir / "ludnosc.csv",
        base_dir / "Ludnosc.csv",
        base_dir / "ludnosc_miejscowosci.csv",
        base_dir / "ludnosc_miejscowosci_uzupelnione_2025.csv",
        Path(__file__).resolve().parent / "ludnosc.csv",
        Path.cwd() / "ludnosc.csv",
    ]
    for p in candidates:
        try:
            if p.exists():
                return p.resolve()
        except Exception:
            pass
    return None


class PopulationResolver:
    """
    Prosty resolver ludności na bazie lokalnego ludnosc.csv.
    Klucz: woj|pow|gmi|mia|dzl (wszystko po kanonizacji).
    """
    def __init__(self, local_csv: Optional[Path]):
        self.local_csv = local_csv
        self._local: Dict[str, float] = {}
        if local_csv:
            self._load_local()

    def _make_key(self, woj="", powiat="", gmina="", miejscowosc="", dzielnica="") -> str:
        w = _canon_admin(woj, "woj")
        p = _canon_admin(powiat, "pow")
        g = _canon_admin(gmina, "gmi")
        m = _canon_admin(miejscowosc, "mia")
        d = _canon_admin(dzielnica, "dzl")
        return "|".join([w, p, g, m, d])

    def _candidate_keys(self, woj: str, powiat: str, gmina: str, miejscowosc: str, dzielnica: str) -> List[str]:
        # hierarchia: dokładnie -> bez dzielnicy -> gmina -> powiat -> woj
        return [
            self._make_key(woj, powiat, gmina, miejscowosc, dzielnica),
            self._make_key(woj, powiat, gmina, miejscowosc, ""),
            self._make_key(woj, powiat, gmina, "", ""),
            self._make_key(woj, powiat, "", "", ""),
            self._make_key(woj, "", "", "", ""),
        ]

    def _load_local(self) -> None:
        try:
            df = pd.read_csv(self.local_csv, sep=";", encoding="utf-8", engine="python")
        except Exception:
            # czasem bywa utf-8-sig
            df = pd.read_csv(self.local_csv, sep=";", encoding="utf-8-sig", engine="python")

        # oczekiwane kolumny (format jak u Ciebie)
        c_woj = _find_col(df.columns, ["Wojewodztwo", "Województwo"])
        c_pow = _find_col(df.columns, ["Powiat"])
        c_gmi = _find_col(df.columns, ["Gmina"])
        c_mia = _find_col(df.columns, ["Miejscowosc", "Miejscowość", "Miasto"])
        c_dzl = _find_col(df.columns, ["Dzielnica", "Osiedle"])
        c_pop = _find_col(df.columns, ["ludnosc", "ludność", "Ludnosc", "Ludność"])

        if not c_woj or not c_mia or not c_pop:
            raise ManualUserError(f"ludnosc.csv ma nieoczekiwany format (brak kolumn: woj/miejscowosc/ludnosc): {self.local_csv}")

        # brakujące admin-y nie blokują działania
        for _, r in df.iterrows():
            woj = r[c_woj] if c_woj else ""
            powiat = r[c_pow] if c_pow else ""
            gmina = r[c_gmi] if c_gmi else ""
            mia = r[c_mia] if c_mia else ""
            dzl = r[c_dzl] if c_dzl else ""
            pop = _to_float_maybe(r[c_pop])
            if pop is None:
                continue
            key = self._make_key(str(woj), str(powiat), str(gmina), str(mia), str(dzl))
            self._local[key] = float(pop)

    def get_population(self, woj="", powiat="", gmina="", miejscowosc="", dzielnica="") -> Optional[float]:
        if not self._local:
            return None
        for k in self._candidate_keys(woj, powiat, gmina, miejscowosc, dzielnica):
            if k in self._local:
                return float(self._local[k])
        return None


# =========================
# Polska.xlsx indeks
# =========================

@dataclass
class PolskaIndex:
    df: pd.DataFrame
    col_area: str
    col_price: str
    c_area_num: str
    c_price_num: str
    col_woj: Optional[str]
    col_pow: Optional[str]
    col_gmi: Optional[str]
    col_mia: Optional[str]
    col_dzl: Optional[str]
    c_woj: Optional[str]
    c_pow: Optional[str]
    c_gmi: Optional[str]
    c_mia: Optional[str]
    c_dzl: Optional[str]
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]]
    by_powiat: Dict[Tuple[str, str], Dict[str, str]]
    by_woj: Dict[str, Dict[str, str]]


def build_polska_index(df_pl: pd.DataFrame, col_area: str, col_price: str) -> PolskaIndex:
    df = df_pl.copy()

    # numeryczne pola
    c_area_num = "_area_num"
    c_price_num = "_price_num"
    df[c_area_num] = df[col_area].map(_to_float_maybe)
    df[c_price_num] = df[col_price].map(_to_float_maybe)

    # kolumny lokalizacji w Polska.xlsx
    col_woj = _find_col(df.columns, ["wojewodztwo", "województwo"])
    col_pow = _find_col(df.columns, ["powiat"])
    col_gmi = _find_col(df.columns, ["gmina"])
    col_mia = _find_col(df.columns, ["miejscowosc", "miejscowość", "miasto"])
    col_dzl = _find_col(df.columns, ["dzielnica", "osiedle"])

    # kanonizacja do porównań
    c_woj = c_pow = c_gmi = c_mia = c_dzl = None
    if col_woj:
        c_woj = "_woj_c"
        df[c_woj] = df[col_woj].map(lambda x: _canon_admin(x, "woj"))
    if col_pow:
        c_pow = "_pow_c"
        df[c_pow] = df[col_pow].map(lambda x: _canon_admin(x, "pow"))
    if col_gmi:
        c_gmi = "_gmi_c"
        df[c_gmi] = df[col_gmi].map(lambda x: _canon_admin(x, "gmi"))
    if col_mia:
        c_mia = "_mia_c"
        df[c_mia] = df[col_mia].map(lambda x: _canon_admin(x, "mia"))
    if col_dzl:
        c_dzl = "_dzl_c"
        df[c_dzl] = df[col_dzl].map(lambda x: _canon_admin(x, "dzl"))

    # mapy miejscowości istniejących w Polska.xlsx
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    by_powiat: Dict[Tuple[str, str], Dict[str, str]] = {}
    by_woj: Dict[str, Dict[str, str]] = {}

    if c_woj and c_mia and col_mia:
        # woj
        for w, gdf in df.groupby(c_woj, dropna=False):
            if not w:
                continue
            mp: Dict[str, str] = {}
            for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                if not mia_c:
                    continue
                try:
                    ex = sub[col_mia].dropna().iloc[0]
                    mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                except Exception:
                    mp[mia_c] = str(mia_c)
            by_woj[str(w)] = mp

        # powiat
        if c_pow and col_pow:
            for (w, p), gdf in df.groupby([c_woj, c_pow], dropna=False):
                if not w or not p:
                    continue
                mp: Dict[str, str] = {}
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
        if c_pow and c_gmi and col_pow and col_gmi:
            for (w, p, g), gdf in df.groupby([c_woj, c_pow, c_gmi], dropna=False):
                if not w or not p or not g:
                    continue
                mp: Dict[str, str] = {}
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
        df=df,
        col_area=col_area,
        col_price=col_price,
        c_area_num=c_area_num,
        c_price_num=c_price_num,
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


# =========================
# Selekcja porównywalnych
# =========================

def _mask_eq_canon(df: pd.DataFrame, canon_col: Optional[str], canon_value: str) -> pd.Series:
    if not canon_col or not canon_value:
        return pd.Series(True, index=df.index)
    return df[canon_col].astype(str) == str(canon_value)


def _filter_miejscowosci_by_bucket(
    candidates: Dict[str, str],
    bucket_low: Optional[int],
    bucket_high: Optional[int],
    pop_resolver: Optional[PopulationResolver],
    woj_raw: str,
    pow_raw: str,
    gmi_raw: str,
    pop_cache: Dict[str, Optional[float]],
) -> List[str]:
    """
    candidates: {mia_canon: mia_original}
    Zwraca listę mia_canon, których ludność mieści się w bucket_low/bucket_high.
    """
    if not candidates or pop_resolver is None or bucket_low is None:
        return []
    out: List[str] = []
    for mia_c, mia_orig in candidates.items():
        cache_key = f"{_canon_admin(woj_raw,'woj')}|{_canon_admin(pow_raw,'pow')}|{_canon_admin(gmi_raw,'gmi')}|{mia_c}"
        if cache_key in pop_cache:
            pop = pop_cache[cache_key]
        else:
            pop = pop_resolver.get_population(woj_raw, pow_raw, gmi_raw, mia_orig, "")
            pop_cache[cache_key] = pop
        if pop is None:
            continue
        if bucket_high is None:
            if pop >= bucket_low:
                out.append(mia_c)
        else:
            if bucket_low <= pop < bucket_high:
                out.append(mia_c)
    return out


def select_comparables(
    pl: PolskaIndex,
    woj_c: str,
    pow_c: str,
    gmi_c: str,
    mia_c: str,
    dzl_c: str,
    woj_raw: str,
    pow_raw: str,
    gmi_raw: str,
    low_area: float,
    high_area: float,
    pop_resolver: Optional[PopulationResolver],
    bucket_low: Optional[int],
    bucket_high: Optional[int],
    min_hits: int = 5,
) -> Tuple[pd.DataFrame, str]:
    """
    Zwraca (df_sel, stage_label).
    df_sel zawiera tylko rekordy w zakresie metrażu oraz z ceną.
    """
    df = pl.df

    # baza: metraż + cena niepusta
    base = pd.Series(True, index=df.index)
    base &= df[pl.c_area_num].notna()
    base &= df[pl.c_price_num].notna()
    base &= (df[pl.c_area_num] >= float(low_area)) & (df[pl.c_area_num] <= float(high_area))

    # helper
    def _take(mask: pd.Series, label: str) -> Tuple[pd.DataFrame, str]:
        sel = df[mask].copy()
        return sel, label

    # 1) DZIELNICA
    if woj_c and mia_c and dzl_c and pl.c_woj and pl.c_mia and pl.c_dzl:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        mask &= _mask_eq_canon(df, pl.c_dzl, dzl_c)
        sel, label = _take(mask, "dzielnica")
        if len(sel.index) >= min_hits:
            return sel, label

    # 2) MIEJSCOWOŚĆ
    if woj_c and mia_c and pl.c_woj and pl.c_mia:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        sel, label = _take(mask, "miejscowosc")
        if len(sel.index) >= min_hits:
            return sel, label

    pop_cache: Dict[str, Optional[float]] = {}

    # 3) GMINA(pop) — miejscowości z tego samego progu ludności
    if woj_c and pow_c and gmi_c and pl.by_gmina and pl.c_woj and pl.c_pow and pl.c_gmi and pl.c_mia:
        candidates = pl.by_gmina.get((woj_c, pow_c, gmi_c), {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw=pow_raw, gmi_raw=gmi_raw,
            pop_cache=pop_cache,
        )
        if not bucket_mias:
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= _mask_eq_canon(df, pl.c_pow, pow_c)
            mask &= _mask_eq_canon(df, pl.c_gmi, gmi_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "gmina(pop)")
            if len(sel.index) >= min_hits:
                return sel, label

    # 4) POWIAT(pop)
    if woj_c and pow_c and pl.by_powiat and pl.c_woj and pl.c_pow and pl.c_mia:
        candidates = pl.by_powiat.get((woj_c, pow_c), {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw=pow_raw, gmi_raw="",
            pop_cache=pop_cache,
        )
        if not bucket_mias:
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= _mask_eq_canon(df, pl.c_pow, pow_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "powiat(pop)")
            if len(sel.index) >= min_hits:
                return sel, label

    # 5) WOJEWÓDZTWO(pop) — wyjątek mazowieckie: sąsiednie bez mazowieckiego, zbieraj wszystkie
    if woj_c and pl.by_woj and pl.c_woj and pl.c_mia:
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
                    pop_cache=pop_cache,
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
                sel = sel.loc[~sel.index.duplicated(keep="first")].copy()
                # nie przerywamy po min_hits — ma być pełny zbiór z sąsiadów
                if len(sel.index) >= min_hits:
                    return sel, "woj_sasiednie(pop)"
                return sel, "woj_sasiednie(pop)_malo"

        # standard: województwo własne
        candidates = pl.by_woj.get(woj_c, {})
        bucket_mias = _filter_miejscowosci_by_bucket(
            candidates, bucket_low, bucket_high, pop_resolver,
            woj_raw=woj_raw, pow_raw="", gmi_raw="",
            pop_cache=pop_cache,
        )
        if not bucket_mias:
            bucket_mias = list(candidates.keys())

        if bucket_mias:
            mask = base.copy()
            mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
            mask &= df[pl.c_mia].isin(bucket_mias)
            sel, label = _take(mask, "woj(pop)")
            if len(sel.index) >= min_hits:
                return sel, label

    # fallback: woj+miejscowość bez progu (jeśli progi odfiltrowały wszystko)
    if woj_c and mia_c and pl.c_woj and pl.c_mia:
        mask = base.copy()
        mask &= _mask_eq_canon(df, pl.c_woj, woj_c)
        mask &= _mask_eq_canon(df, pl.c_mia, mia_c)
        sel, label = _take(mask, "miejscowosc(fallback)")
        if not sel.empty:
            return sel, label

    return df.iloc[0:0].copy(), "brak"


# =========================
# Cache (żeby nie czytać Polska.xlsx za każdym kliknięciem)
# =========================

_CACHE: Dict[str, Any] = {
    "polska_path": None,
    "polska_mtime": None,
    "df_pl": None,
    "pl_index": None,
    "ludnosc_path": None,
    "ludnosc_mtime": None,
    "pop_resolver": None,
}


def _get_cached_index_and_pop(base_dir: Path) -> Tuple[PolskaIndex, Optional[PopulationResolver]]:
    polska_path = (base_dir / "Polska.xlsx").resolve()
    if not polska_path.exists():
        raise ManualUserError(f"Nie znaleziono pliku: {polska_path}")

    polska_mtime = polska_path.stat().st_mtime

    # ludnosc
    ludnosc_path = _find_ludnosc_csv(base_dir)
    ludnosc_mtime = ludnosc_path.stat().st_mtime if ludnosc_path and ludnosc_path.exists() else None

    # Polska.xlsx cache
    need_reload_polska = (_CACHE["polska_path"] != str(polska_path)) or (_CACHE["polska_mtime"] != polska_mtime)
    if need_reload_polska:
        df_pl = pd.read_excel(polska_path)
        col_area_pl = _find_col(df_pl.columns, ["metry", "powierzchnia", "Obszar", "obszar"])
        col_price_pl = _find_col(df_pl.columns, ["cena_za_metr", "cena za metr", "cena za m²", "cena za m2", "cena/m2", "cena_za_m2"])
        if not col_area_pl or not col_price_pl:
            raise ManualUserError("Polska.xlsx nie zawiera wymaganych kolumn metrażu i/lub ceny za m².")
        pl_index = build_polska_index(df_pl, col_area_pl, col_price_pl)
        _CACHE.update({
            "polska_path": str(polska_path),
            "polska_mtime": polska_mtime,
            "df_pl": df_pl,
            "pl_index": pl_index,
        })

    # ludnosc cache
    need_reload_ludnosc = False
    if ludnosc_path is None:
        if _CACHE.get("pop_resolver") is None:
            need_reload_ludnosc = True
    else:
        if (_CACHE.get("ludnosc_path") != str(ludnosc_path)) or (_CACHE.get("ludnosc_mtime") != ludnosc_mtime):
            need_reload_ludnosc = True

    if need_reload_ludnosc:
        pop_resolver = PopulationResolver(ludnosc_path) if ludnosc_path else None
        _CACHE.update({
            "ludnosc_path": str(ludnosc_path) if ludnosc_path else None,
            "ludnosc_mtime": ludnosc_mtime,
            "pop_resolver": pop_resolver,
        })

    return _CACHE["pl_index"], _CACHE.get("pop_resolver")


# =========================
# Główna funkcja dla selektor_csv.py
# =========================

def compute_and_save_row(
    df_report: pd.DataFrame,
    idx: int,
    base_dir: Path,
    out_dir: Path,
    margin_m2_default: float = 15.0,
    margin_pct_default: float = 15.0,
    min_hits: int = 5,
) -> Dict[str, Any]:
    """
    Liczy i zapisuje wybrany wiersz raportu:
    - dobiera ogłoszenia wg nowego algorytmu
    - zapisuje plik (Nr KW).xlsx do out_dir
    - wpisuje wyniki do df_report (w miejscu)

    Zwraca słownik z detalami (avg, corrected, value, out_path, stage, hits, pop, bucket).
    """
    if df_report is None or idx is None:
        raise ManualUserError("Brak raportu lub indeksu wiersza.")
    if idx < 0 or idx >= len(df_report.index):
        raise ManualUserError("Nieprawidłowy indeks wiersza.")

    row = df_report.iloc[idx]

    # --- pola z raportu ---
    kw_col = _find_col(df_report.columns, ["Nr KW", "nr_kw", "nrksiegi", "nr księgi", "nr_ksiegi", "numer księgi"])
    kw_value = (str(row[kw_col]).strip() if (kw_col and pd.notna(row[kw_col]) and str(row[kw_col]).strip()) else f"WIERSZ_{idx+1}")

    area_col = _find_col(df_report.columns, ["Obszar", "metry", "powierzchnia"])
    area_val = _to_float_maybe(_trim_after_semicolon(row[area_col])) if area_col else None
    if area_val is None:
        raise ManualUserError("Nie znalazłem wartości obszaru/metry w raporcie (dla tego wiersza).")

    def _get(cands):
        c = _find_col(df_report.columns, cands)
        return _trim_after_semicolon(row[c]) if c else ""

    woj_r = _get(["Województwo", "Wojewodztwo", "wojewodztwo", "woj"])
    pow_r = _get(["Powiat"])
    gmi_r = _get(["Gmina"])
    mia_r = _get(["Miejscowość", "Miejscowosc", "Miasto"])
    dzl_r = _get(["Dzielnica", "Osiedle"])

    woj_c = _canon_admin(woj_r, "woj")
    pow_c = _canon_admin(pow_r, "pow")
    gmi_c = _canon_admin(gmi_r, "gmi")
    mia_c = _canon_admin(mia_r, "mia")
    dzl_c = _canon_admin(dzl_r, "dzl")

    # kolumny wynikowe w raporcie
    mean_col = _find_col(df_report.columns, ["Średnia cena za m2 ( z bazy)", "Srednia cena za m2 ( z bazy)", "Średnia cena za m² ( z bazy)"])
    corr_col = _find_col(df_report.columns, ["Średnia skorygowana cena za m2", "Srednia skorygowana cena za m2"])
    val_col = _find_col(df_report.columns, ["Statystyczna wartość nieruchomości", "Statystyczna wartosc nieruchomosci"])

    if mean_col is None:
        mean_col = "Średnia cena za m2 ( z bazy)"
        if mean_col not in df_report.columns:
            df_report[mean_col] = ""
    if corr_col is None:
        corr_col = "Średnia skorygowana cena za m2"
        if corr_col not in df_report.columns:
            df_report[corr_col] = ""
    if val_col is None:
        val_col = "Statystyczna wartość nieruchomości"
        if val_col not in df_report.columns:
            df_report[val_col] = ""

    # minimalne dane: woj + miejscowość
    STRICT_MSG = "BRAK LUB NIEPEŁNY ADRESU – WPISZ ADRES MANUALNIE"
    if not woj_c or not mia_c:
        df_report.at[idx, mean_col] = STRICT_MSG
        df_report.at[idx, corr_col] = STRICT_MSG
        df_report.at[idx, val_col] = STRICT_MSG
        return {
            "kw": kw_value, "avg": None, "corrected": None, "value": None,
            "out_path": None, "stage": "strict", "hits": 0,
            "pop": None, "bucket": None,
        }

    # cache: Polska.xlsx index + ludnosc
    pl_index, pop_resolver = _get_cached_index_and_pop(base_dir)

    # ludność + progi
    pop_target = pop_resolver.get_population(woj_r, pow_r, gmi_r, mia_r, dzl_r) if pop_resolver else None
    bucket_low, bucket_high = bucket_for_population(pop_target)

    if pop_target is None:
        margin_m2_row, margin_pct_row = float(margin_m2_default), float(margin_pct_default)
    else:
        margin_m2_row, margin_pct_row = rules_for_population(pop_target)

    delta = abs(float(margin_m2_row or 0.0))
    low_area, high_area = max(0.0, float(area_val) - delta), float(area_val) + delta

    df_sel, stage = select_comparables(
        pl=pl_index,
        woj_c=woj_c,
        pow_c=pow_c,
        gmi_c=gmi_c,
        mia_c=mia_c,
        dzl_c=dzl_c,
        woj_raw=woj_r,
        pow_raw=pow_r,
        gmi_raw=gmi_r,
        low_area=low_area,
        high_area=high_area,
        pop_resolver=pop_resolver,
        bucket_low=bucket_low,
        bucket_high=bucket_high,
        min_hits=int(min_hits),
    )

    if df_sel.empty:
        msg = "BRAK OGŁOSZEŃ W BAZIE DLA TEGO ZAKRESU"
        df_report.at[idx, mean_col] = msg
        df_report.at[idx, corr_col] = msg
        df_report.at[idx, val_col] = msg
        return {
            "kw": kw_value, "avg": None, "corrected": None, "value": None,
            "out_path": None, "stage": stage, "hits": 0,
            "pop": pop_target, "bucket": (bucket_low, bucket_high),
            "area_range": (low_area, high_area),
        }

    # outliers — zawsze usuwamy wartości brzegowe w wyliczeniach
    df_sel, _prices_arr = _filter_outliers_df(df_sel, pl_index.c_price_num)
    prices = _prices_arr
    mean_price = float(np.nanmean(prices))
    mean_rounded = round(float(mean_price), 2)

    corrected = mean_rounded * (1.0 - float(margin_pct_row or 0.0) / 100.0)
    corrected_rounded = round(float(corrected), 2)

    value = corrected_rounded * float(area_val)
    value_rounded = round(float(value), 2)

    # wpis do raportu
    df_report.at[idx, mean_col] = mean_rounded
    df_report.at[idx, corr_col] = corrected_rounded
    df_report.at[idx, val_col] = value_rounded

    # zapis pliku (Nr KW).xlsx z wybranymi rekordami + średnia na dole
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_kw = "".join(ch for ch in str(kw_value) if ch not in "\\/:*?\"<>|")
    out_path = out_dir / f"({safe_kw}).xlsx"

    df_out = df_sel.copy()

    # kolumna pomocnicza wiersza średniej
    summary = {c: "" for c in df_out.columns}
    # wpisz średnią do kolumny ceny za metr, jeśli istnieje (oryginalnej)
    try:
        summary[pl_index.col_price] = mean_rounded
    except Exception:
        pass

    df_out = pd.concat([df_out, pd.DataFrame([summary])], ignore_index=True)
    df_out.loc[len(df_out) - 1, "ŚREDNIA_CENA_M2"] = mean_rounded
    df_out.loc[len(df_out) - 1, "ETAP_DOBORU"] = stage
    df_out.loc[len(df_out) - 1, "HITS"] = int(len(df_sel))

    # sensowne kolumny (jeśli istnieją)
    premium_cols = [
        "cena", "cena_za_metr", "cena_za_m2", "metry", "powierzchnia", "liczba_pokoi", "pietro",
        "rynek", "rok_budowy", "material",
        "wojewodztwo", "powiat", "gmina", "miejscowosc", "miejscowość", "dzielnica", "ulica",
        "link",
        "ŚREDNIA_CENA_M2", "ETAP_DOBORU", "HITS",
    ]
    existing = [c for c in premium_cols if c in df_out.columns]
    if existing:
        # zachowaj też inne kolumny z df_out, jeśli premium nie zawiera ceny/metrażu w Twojej wersji
        df_out = df_out[existing + [c for c in df_out.columns if c not in existing]]

    df_out.to_excel(out_path, index=False)

    return {
        "kw": kw_value,
        "avg": mean_rounded,
        "corrected": corrected_rounded,
        "value": value_rounded,
        "out_path": out_path,
        "stage": stage,
        "hits": int(len(df_sel)),
        "pop": pop_target,
        "bucket": (bucket_low, bucket_high),
        "area_range": (low_area, high_area),
        "margins": (margin_m2_row, margin_pct_row),
    }

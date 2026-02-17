# -*- coding: utf-8 -*-
"""
linki_mieszkania.py
Zbiera linki do ofert "SPRZEDAŻ / MIESZKANIE" z Otodom dla wskazanego województwa.

Użycie:
  python linki_mieszkania.py --region podlaskie --output podlaskie.csv
Opcjonalnie:
  --per_page 72        (domyślnie 72)
  --delay 0.60         (opóźnienie między stronami)
  --max_pages N        (na potrzeby testów)
"""

from __future__ import annotations
import argparse
import csv
import re
import sys
import time
import unicodedata
from datetime import datetime
from math import ceil
from pathlib import Path
from typing import Iterable, Set, List
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

UA = "Chrome/127.0.0.0"


def LOG(msg: str) -> None:
    print(msg, flush=True)


def normalize_region_slug(name: str) -> str:
    """
    Normalizacja nazwy województwa do sluga używanego przez Otodom:
      - małe litery
      - bez polskich znaków
      - spacje -> '--'
      - KAŻDY pojedynczy '-' między znakami słowa -> '--' (np. 'warminsko-mazurskie' -> 'warminsko--mazurskie')
      - tylko [a-z0-9-], bez znaków specjalnych
    """
    s = (name or "").strip().lower()
    # usuń diakrytyki
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    # spacje -> '--'
    s = re.sub(r"\s+", "--", s)
    # pojedynczy '-' pomiędzy znakami słowa -> '--'
    s = re.sub(r"(?<=\w)-(?=\w)", "--", s)
    # dopuszczalne: litery, cyfry, '-'
    s = re.sub(r"[^a-z0-9\-]+", "", s)
    # zredukuj 3+ minusy do dokładnie dwóch
    s = re.sub(r"-{3,}", "--", s)
    return s.strip("-")


def mk_session() -> requests.Session:
    s = requests.Session()
    headers = {
        "User-Agent": UA,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.otodom.pl/",
    }
    LOG(f"[HTTP] Headers: {headers}")
    s.headers.update(headers)
    return s


def soup_of(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


_BANNER_RE = re.compile(
    r"(\d+)\s*[-–]\s*(\d+)\s+og(?:ł|l)osze(?:ń|n)\s+z\s+(\d+)",
    re.IGNORECASE
)


def _int(s: str) -> int:
    return int(re.sub(r"\D", "", s)) if s else 0


def parse_banner_counts(html: str) -> tuple[int, int, int] | None:
    """
    Zwraca (lo, hi, total) z tekstu '1-72 ogłoszeń z 2798'.
    """
    m = _BANNER_RE.search(html.replace("\xa0", " "))
    if not m:
        return None
    lo = _int(m.group(1))
    hi = _int(m.group(2))
    total = _int(m.group(3))
    return lo, hi, total


def clean_url(u: str, base: str = "https://www.otodom.pl") -> str:
    """
    Normalizuje link oferty:
      - absolutny URL
      - bez query (UTM itd.)
      - zachowuje ścieżkę /pl/oferta/...
    """
    if not u:
        return ""
    absu = urljoin(base, u)
    parts = urlsplit(absu)
    # akceptuj tylko ścieżki z /pl/oferta/
    if "/pl/oferta/" not in parts.path:
        return ""
    # bez parametrów query/fragment i bez trailing slash
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def extract_links(html: str) -> List[str]:
    """
    Główna metoda: DOM — selektor 'a[data-cy="listing-item-link"]'.
    Fallback: 'a[href*="/pl/oferta/"]'
    """
    sp = soup_of(html)
    links: list[str] = []

    # wariant podstawowy
    for a in sp.select('a[data-cy="listing-item-link"]'):
        href = a.get("href", "")
        u = clean_url(href)
        if u:
            links.append(u)

    # fallback, gdyby data-cy się zmieniło
    if not links:
        for a in sp.select('a[href*="/pl/oferta/"]'):
            href = a.get("href", "")
            u = clean_url(href)
            if u:
                links.append(u)

    return links


def unique(seq: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for u in seq:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def page_url(region_slug: str, page: int, per_page: int) -> str:
    return (
        f"https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/"
        f"{region_slug}?limit={per_page}&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&page={page}"
    )


def fetch(sess: requests.Session, url: str) -> str:
    LOG(f"[GET] {url}")
    r = sess.get(url, timeout=(10, 30), allow_redirects=True)
    LOG(f"[HTTP] status={r.status_code} final_url={r.url} len={len(r.text)}")
    r.raise_for_status()
    return r.text


def _read_existing_links(out_csv: Path) -> tuple[list[str], set[str]]:
    """Czyta już zapisane linki (bez nagłówka). Zwraca (lista_w_kolejnosci, set)."""
    if not out_csv.exists():
        return [], set()
    links: list[str] = []
    seen: set[str] = set()
    try:
        with out_csv.open("r", encoding="utf-8-sig", newline="") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                if i == 0 and line.lower().startswith("link"):
                    continue
                u = line
                if u and u not in seen:
                    seen.add(u)
                    links.append(u)
    except Exception as e:
        LOG(f"[WARN] Nie mogę odczytać istniejącego CSV: {e}")
    return links, seen


def _append_new_links(out_csv: Path, new_links: list[str]) -> None:
    """Dopisuje linki do CSV (1 kolumna), dodając nagłówek jeśli plik pusty/nie istnieje."""
    if not new_links:
        return
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = (not out_csv.exists()) or (out_csv.stat().st_size == 0)
    with out_csv.open("a", encoding="utf-8-sig", newline="") as f:
        if write_header:
            f.write("link\n")
        for u in new_links:
            f.write(u + "\n")


def _load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {}
    try:
        import json
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state_path: Path, data: dict) -> None:
    try:
        import json
        state_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", required=True, help="np. 'podlaskie' (bez polskich znaków też może być)")
    ap.add_argument("--output", required=True, help="ścieżka do CSV z linkami (1 kolumna: link)")
    ap.add_argument("--per_page", type=int, default=72)
    ap.add_argument("--delay", type=float, default=0.60)
    ap.add_argument("--max_pages", type=int, default=0, help="0 = wg banera; >0 ogranicza liczbę stron")
    args = ap.parse_args()

    region_input = args.region
    region_slug = normalize_region_slug(region_input)
    out_csv = Path(args.output).resolve()

    stop_path  = out_csv.with_suffix(".stop")
    state_path = out_csv.with_suffix(".state.json")
    done_path  = out_csv.with_suffix(".done")

    LOG(f"[start] region='{region_input}' type='mieszkanie' output='{out_csv}'")
    LOG(f"[slug] '{region_input}' -> '{region_slug}'")

    # Jeśli już gotowe — nic nie rób
    if done_path.exists():
        LOG(f"[done] marker istnieje: {done_path} (linki już zebrane)")
        return

    # Wczytaj to co już masz + stan wznowienia
    existing_list, existing_set = _read_existing_links(out_csv)
    st = _load_state(state_path)

    next_page = int(st.get("next_page", 1) or 1)
    max_pages = int(st.get("max_pages", 0) or 0)

    LOG(f"[resume] existing_unique={len(existing_set)} next_page={next_page} max_pages(state)={max_pages}")

    sess = mk_session()

    # Ustal max_pages (jeśli nie mamy ze stanu)
    if max_pages <= 0:
        url1 = page_url(region_slug, 1, args.per_page)
        LOG(f"[URL p1] {url1}")
        html1 = fetch(sess, url1)

        bc = parse_banner_counts(html1)
        if bc:
            lo, hi, total = bc
            LOG(f"[baner] {lo}-{hi} ogłoszeń z {total} -> total={total}")
            max_pages = ceil(total / args.per_page)
            LOG(f"[pages] total={total} per_page={args.per_page} -> max_pages={max_pages}")
        else:
            LOG("[WARN] Nie udało się znaleźć banera — przyjmuję 1 stronę")
            max_pages = 1

        if args.max_pages and args.max_pages > 0:
            max_pages = min(max_pages, args.max_pages)
            LOG(f"[limit] max_pages ograniczone do {max_pages}")

        # jeśli startujemy od 1 — od razu przerób stronę 1, żeby nie robić GET drugi raz
        if next_page <= 1:
            if stop_path.exists():
                LOG(f"[STOP] wykryto {stop_path} — kończę przed stroną 1")
                _save_state(state_path, {"region": region_input, "region_slug": region_slug,
                                        "per_page": args.per_page, "max_pages": max_pages,
                                        "next_page": 1, "unique": len(existing_set)})
                return

            links1 = extract_links(html1)
            new_links = []
            for u in links1:
                if u and u not in existing_set:
                    existing_set.add(u)
                    new_links.append(u)
            _append_new_links(out_csv, new_links)
            LOG(f"[page 1] dom={len(links1)} new={len(new_links)} total_unique={len(existing_set)}")

            next_page = 2

    # jeśli user wymusił limit w trakcie wznowienia
    if args.max_pages and args.max_pages > 0:
        max_pages = min(max_pages, args.max_pages)

    # Główna pętla od next_page
    for p in range(next_page, max_pages + 1):
        if stop_path.exists():
            LOG(f"[STOP] wykryto {stop_path} — kończę po stronie {p-1}")
            _save_state(state_path, {"region": region_input, "region_slug": region_slug,
                                    "per_page": args.per_page, "max_pages": max_pages,
                                    "next_page": p, "unique": len(existing_set)})
            return

        urlp = page_url(region_slug, p, args.per_page)
        html = fetch(sess, urlp)
        links = extract_links(html)

        new_links = []
        for u in links:
            if u and u not in existing_set:
                existing_set.add(u)
                new_links.append(u)

        _append_new_links(out_csv, new_links)
        LOG(f"[page {p}] dom={len(links)} new={len(new_links)} total_unique={len(existing_set)}")

        _save_state(state_path, {"region": region_input, "region_slug": region_slug,
                                "per_page": args.per_page, "max_pages": max_pages,
                                "next_page": p + 1, "unique": len(existing_set)})

        if args.delay > 0:
            LOG(f"[sleep] {args.delay:.2f}s")
            time.sleep(args.delay)

    # Koniec: linki kompletne
    try:
        done_path.write_text(datetime.now().isoformat(), encoding="utf-8")
    except Exception:
        # fallback: "touch"
        try:
            done_path.touch(exist_ok=True)
        except Exception:
            pass

    # sprzątanie stanu/stop
    try:
        if state_path.exists():
            state_path.unlink()
    except Exception:
        pass
    try:
        if stop_path.exists():
            stop_path.unlink()
    except Exception:
        pass

    LOG(f"[done] zapisano: {out_csv} (unikalnych linków: {len(existing_set)}) marker={done_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOG(f"[ERR] {e}")
        sys.exit(1)

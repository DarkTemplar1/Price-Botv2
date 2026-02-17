# -*- coding: utf-8 -*-
"""
Zbiera linki z wyników Otodom.
- region: polska nazwa lub slug (np. "Podlaskie" albo "podlaskie")
- typ: mieszkanie/dom (domyślnie mieszkanie)
- zatrzymuje się, gdy na 2 kolejnych stronach nie ma nowych ogłoszeń
- zapisuje 1 link na linię (bez nagłówka), w formie absolutnej:
  https://www.otodom.pl/pl/oferta/...
"""

from __future__ import annotations
import argparse, csv, re, sys, time
from pathlib import Path
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Referer": "https://www.otodom.pl/",
}

VOIVODESHIP_SLUG = {
    "Dolnośląskie": "dolnoslaskie",
    "Kujawsko-Pomorskie": "kujawsko--pomorskie",
    "Lubelskie": "lubelskie",
    "Lubuskie": "lubuskie",
    "Łódzkie": "lodzkie",
    "Małopolskie": "malopolskie",
    "Mazowieckie": "mazowieckie",
    "Opolskie": "opolskie",
    "Podkarpackie": "podkarpackie",
    "Podlaskie": "podlaskie",
    "Pomorskie": "pomorskie",
    "Śląskie": "slaskie",
    "Świętokrzyskie": "swietokrzyskie",
    "Warmińsko-Mazurskie": "warminsko--mazurskie",
    "Wielkopolskie": "wielkopolskie",
    "Zachodniopomorskie": "zachodniopomorskie",
}

def resolve_desktop_dir() -> Path:
    home = Path.home()
    for d in [home/"Desktop", home/"Pulpit", home/"OneDrive"/"Desktop"]:
        if d.exists(): return d
    return home

def normalize_url(u: str) -> str | None:
    if not u: return None
    u = u.strip()
    # /pl/oferta/... -> absolutny
    if u.startswith("//"):
        u = "https:" + u
    elif u.startswith("/"):
        u = "https://www.otodom.pl" + u
    elif u.startswith("www.otodom.pl"):
        u = "https://" + u
    # kanon + pojedynczy ukośnik
    u = u.replace("https://www.otodom.pl/hpr", "https://www.otodom.pl")
    u = re.sub(r"https://www\.otodom\.pl/+", "https://www.otodom.pl/", u)
    # tylko oferty
    return u if "/pl/oferta/" in u else None

def fetch(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", required=True, help="np. Podlaskie lub podlaskie")
    ap.add_argument("--type", choices=["mieszkanie","dom"], default="mieszkanie")
    ap.add_argument("--output", help="gdzie zapisać listę linków (CSV bez nagłówka)")
    ap.add_argument("--sleep", type=float, default=0.6)
    ap.add_argument("--max-pages", type=int, default=500)
    args = ap.parse_args()

    # slug
    region = args.region
    slug = VOIVODESHIP_SLUG.get(region, region.lower())
    base_dir = resolve_desktop_dir() / "baza danych" / "linki"
    base_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.output) if args.output else base_dir / f"{region}.csv"

    page = 1
    seen: set[str] = set()
    no_new_pages = 0

    print(f"[start] region='{slug}' type='{args.type}' output='{out_path}'")
    while page <= args.max_pages and no_new_pages < 2:
        url = f"https://www.otodom.pl/pl/wyniki/sprzedaz/{args.type}/{slug}?limit=72&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&page={page}"
        soup = fetch(url)

        # linki z kafelków
        found = set()
        for a in soup.select("a"):
            href = a.get("href", "")
            if not href: continue
            u = normalize_url(href)
            if not u: continue
            found.add(u)

        new = [u for u in found if u not in seen]
        seen.update(new)

        print(f"[page {page}] new={len(new)} total_unique={len(seen)}", flush=True)
        if len(new) == 0:
            no_new_pages += 1
        else:
            no_new_pages = 0

        page += 1
        time.sleep(args.sleep)

    # zapis bez nagłówka – jeden URL na linię
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for u in sorted(seen):
            w.writerow([u])

    print(f"[done] zapisano {len(seen)} linków do {out_path}")

if __name__ == "__main__":
    sys.exit(main())

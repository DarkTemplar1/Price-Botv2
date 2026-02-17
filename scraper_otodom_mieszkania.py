#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import random
import re
import time
import unicodedata
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ====== KONFIG ======
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

FIELDS = [
    "cena","cena_za_metr","metry","liczba_pokoi","pietro","rynek","rok_budowy",
    "material","wojewodztwo","powiat","gmina","miejscowosc","dzielnica","ulica","link"
]

FLOOR_MAP  = {"ground_floor": "parter", "basement": "suterena", "loft": "poddasze"}
MARKET_MAP = {"primary": "pierwotny", "secondary": "wtórny"}


# ====== FILTRY "UI ŚMIECI" (żeby nie zapisywać do CSV złapanych przycisków/CTA/map itp.) ======

_UI_BLACKLIST_RAW = {
    # ogólne przyciski/CTA
    "wróć", "wroc", "udostępnij", "udostepnij", "zapisz", "obserwuj",
    "wszystkie zdjęcia", "wszystkie zdjecia", "pokaż na mapie", "pokaz na mapie",
    "zadzwoń", "zadzwon", "napisz", "drukuj", "pobierz", "pełny ekran", "pelny ekran",
    "galeria", "wideo", "video", "wirtualny spacer",
    # serwisy grupy
    "otomoto.pl", "fixly.pl", "obido.pl", "kupuję nieruchomości", "kupuje nieruchomosci",
    # nawigacja/mapy
    "google", "maps", "openstreetmap", "wyznacz trasę", "wyznacz trase", "trasa", "dojazd",
}

def _norm_txt(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # usuń polskie znaki/diakrytyki
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

_UI_BLACKLIST = {_norm_txt(x) for x in _UI_BLACKLIST_RAW if x}

def _contains_ui_garbage(text: str) -> tuple[bool, str]:
    """Zwraca (True, term) jeśli tekst wygląda jak element UI (np. 'wroc', 'udostepnij', 'pokaz na mapie')."""
    nt = _norm_txt(text)
    if not nt:
        return False, ""
    # sprawdzamy:
    # - frazy wielowyrazowe / z kropkami: substring
    # - pojedyncze tokeny: dopasowanie na granicach (żeby np. 'wroc' nie łapało 'wroclaw')
    for term in _UI_BLACKLIST:
        if " " in term or "." in term or "/" in term:
            if term in nt:
                return True, term
    for term in _UI_BLACKLIST:
        if " " in term or "." in term or "/" in term:
            continue
        if re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", nt):
            return True, term
    return False, ""

def should_skip_row(row: dict) -> tuple[bool, str]:
    """Czy ominąć zapis wiersza do CSV? (True, reason)."""
    if not isinstance(row, dict):
        return True, "row_not_dict"

    # Zostawiamy wiersze ERROR (żeby było wiadomo co padło), chyba że są kompletnie śmieciowe.
    is_error = str(row.get("cena", "")).startswith("ERROR:")


    # Pomijamy ogłoszenia "Zapytaj o cenę" (nie chcemy ich w CSV)
    if not is_error:
        cena_txt = str(row.get("cena", ""))
        ncena = _norm_txt(cena_txt)
        # łapie m.in.: "Zapytaj o cenę", "Zapytaj o cene", "Zapytaj o cenę w biurze"
        if ncena and ("zapytaj" in ncena and "cen" in ncena):
            return True, "price_ask"

    addr_fields = ("wojewodztwo", "powiat", "gmina", "miejscowosc", "dzielnica", "ulica")
    for f in addr_fields:
        hit, term = _contains_ui_garbage(str(row.get(f, "")))
        if hit:
            return (True, f"ui_garbage:{f}:{term}")

    # minimalna walidacja adresu (dla nie-error)
    if not is_error:
        if not str(row.get("wojewodztwo", "")).strip():
            return True, "missing_wojewodztwo"
        if not str(row.get("miejscowosc", "")).strip():
            return True, "missing_miejscowosc"

    return False, ""



# ====== NARZĘDZIA HTML / JSON ======
def extract_next_data(html: str):
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if tag and tag.string:
        try:
            return json.loads(tag.string)
        except Exception:
            pass
    for s in soup.find_all("script", attrs={"type": "application/json"}):
        try:
            obj = json.loads(s.string or "")
            if isinstance(obj, dict) and "props" in obj and "pageProps" in obj["props"]:
                return obj
        except Exception:
            continue
    return None


def deep_iter(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from deep_iter(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from deep_iter(v)


def get_char(characteristics, key, prefer_localized=True):
    if not characteristics:
        return ""
    for ch in characteristics:
        if ch.get("key") == key:
            if prefer_localized and ch.get("localizedValue"):
                return str(ch["localizedValue"]).strip()
            return str(ch.get("value") or "").strip()
    return ""


def pick_name(d, key):
    v = (d or {}).get(key)
    if isinstance(v, dict):
        return v.get("name", "") or v.get("label", "") or ""
    return v or ""


def all_strings(obj, max_len=200):
    seen = set()
    for _k, v in deep_iter(obj):
        if isinstance(v, str):
            s = v.strip()
            if s and len(s) <= max_len and s not in seen:
                seen.add(s)
                yield s


# ====== WYKRYWANIE DZIELNICY (ulepszone) ======
KNOWN_DISTRICTS = [
    "Nowe Miasto","Staromieście","Baranówka","Zalesie","Drabinianka","Budziwój","Słocina",
    "Przybyszówka","Zwięczyca","Wilkowyja","Bacieczki","Bojary","Dziesięciny","Piasta",
]
FRAN_ANY = re.compile(r"\b(Frani\w*\s+Kotuli)\b", re.I)

# heurystyka: jeśli w treści adresu są segmenty "... ul. X, [coś], Miasto"
BETWEEN_STREET_CITY = re.compile(
    r"(ul\.|ulica)?\s*([A-ZŁŚŻŹĆŃ][\w\-\s\.']+)\s*,\s*([A-ZŁŚŻŹĆŃ][\w\-\s\.']+)\s*,\s*([A-ZŁŚŻŹĆŃ][\w\-\s\.']+)",
    re.I
)

def detect_dzielnica(next_data, miasto, ulica):
    text = " | ".join(all_strings(next_data, 300))

    # 1) „coś” pomiędzy ulicą a miastem -> traktuj jako dzielnicę/osiedle
    try:
        for m in BETWEEN_STREET_CITY.finditer(text):
            _ul_lab, ul_name, maybe_dist, city = m.groups()
            if miasto and city and city.lower() == str(miasto).lower():
                if ulica and ul_name and ul_name.lower() in str(ulica).lower():
                    if maybe_dist and maybe_dist.lower() != city.lower():
                        return maybe_dist.strip()
    except Exception:
        pass

    # 2) specjalny case Kotuli/Projektant
    m = FRAN_ANY.search(text)
    if m:
        return m.group(1)

    # 3) lista znanych osiedli
    for name in KNOWN_DISTRICTS:
        if re.search(rf"\b{name}\b", text, flags=re.I):
            return name

    return ""


# ====== PARSOWANIE OGŁOSZENIA ======
def parse_ad(next_data: dict, url: str) -> dict:
    page_props = (next_data.get("props") or {}).get("pageProps", {})
    ad = page_props.get("ad") or {}

    if not ad:
        # fallback – rekursywnie wyszukaj węzeł z 'characteristics' i 'location'
        def walk(d):
            if isinstance(d, dict):
                if "characteristics" in d and "location" in d:
                    return d
                for v in d.values():
                    r = walk(v)
                    if r:
                        return r
            elif isinstance(d, list):
                for v in d:
                    r = walk(v)
                    if r:
                        return r
            return None
        found = walk(page_props)
        if found:
            ad = found

    chars = ad.get("characteristics") or []
    cena = get_char(chars, "price")
    cena_m = get_char(chars, "price_per_m")
    metry = get_char(chars, "m")
    pokoje = get_char(chars, "rooms_num")

    floor_val = get_char(chars, "floor_no", prefer_localized=False)
    pietro = get_char(chars, "floor_no", prefer_localized=True) or FLOOR_MAP.get(floor_val, floor_val)

    rynek_raw = (get_char(chars, "market", prefer_localized=False) or "").lower()
    rynek = MARKET_MAP.get(rynek_raw, get_char(chars, "market", prefer_localized=True))

    rok = get_char(chars, "build_year", prefer_localized=False) or get_char(chars, "build_year")
    material = get_char(chars, "building_material")

    addr = ((ad.get("location") or {}).get("address")) or {}
    woj   = pick_name(addr, "province")
    powiat = pick_name(addr, "county")
    gmina = pick_name(addr, "municipality")
    miasto = pick_name(addr, "city")
    dzielnica = pick_name(addr, "district")
    ulica = pick_name(addr, "street")

    # fallback – spróbuj wyłuskać dane adresowe z innych gałęzi
    if not (woj and gmina and miasto and (dzielnica or ulica)):
        for _k, v in deep_iter(next_data):
            if isinstance(v, dict):
                keys = set(v.keys())
                if {"province","county","municipality","city","district","street"} & keys:
                    woj   = woj   or pick_name(v, "province")
                    powiat = powiat or pick_name(v, "county")
                    gmina = gmina or pick_name(v, "municipality")
                    miasto = miasto or pick_name(v, "city")
                    dzielnica = dzielnica or pick_name(v, "district")
                    ulica = ulica or pick_name(v, "street")

    if not dzielnica:
        dzielnica = detect_dzielnica(next_data, miasto, ulica)

    link = ad.get("url") or url

    return {
        "cena": cena or "",
        "cena_za_metr": cena_m or "",
        "metry": metry or "",
        "liczba_pokoi": pokoje or "",
        "pietro": pietro or "",
        "rynek": rynek or "",
        "rok_budowy": (str(rok) if rok is not None else ""),
        "material": material or "",
        "wojewodztwo": woj or "",
        "powiat": powiat or "",
        "gmina": gmina or "",
        "miejscowosc": miasto or "",
        "dzielnica": dzielnica or "",
        "ulica": ulica or "",
        "link": link or "",
    }


def fetch_one(url: str, session: requests.Session, retries: int = 3, backoff: float = 1.6) -> dict:
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
            data = extract_next_data(r.text)
            if not data:
                raise RuntimeError("Brak __NEXT_DATA__ / pageProps w HTML")
            row = parse_ad(data, url)
            # minimalna walidacja – jeśli nic nie wyciągnęliśmy, oznacz jako błąd
            if not any(row.get(k) for k in ("cena", "metry", "liczba_pokoi")):
                raise RuntimeError("Nie udało się wyciągnąć kluczowych pól")
            return row
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(backoff ** attempt)
            else:
                row = {k: "" for k in FIELDS}
                row["link"] = url
                row["cena"] = f"ERROR: {last_exc}"
                return row


# ====== I/O LINKÓW ======
def guess_region_name_from_path(path: Path) -> str:
    # nazwa pliku bez rozszerzenia (np. Podlaskie.csv -> Podlaskie)
    return path.stem


def read_links_any(input_path: Path) -> list[str]:
    """
    Czyta plik linków:
    - CSV z nagłówkiem 'link' lub pierwszą kolumną URL,
    - albo zwykłą listę URL-i (po 1 w linii).
    """
    links = []
    text = input_path.read_text(encoding="utf-8", errors="ignore")
    # spróbuj CSV
    try:
        rows = list(csv.reader(text.splitlines()))
        if rows:
            hdr = [h.strip().lower() for h in rows[0]]
            start_idx = 1 if any(h in ("link","url") for h in hdr) else 0
            for row in rows[start_idx:]:
                for cell in row:
                    if isinstance(cell, str) and cell.startswith("http"):
                        links.append(cell.strip())
                        break
            if links:
                return dedupe_preserve_order(links)
    except Exception:
        pass
    # wierszowe URL-e
    for ln in text.splitlines():
        ln = ln.strip()
        if ln.startswith("http"):
            links.append(ln)
    return dedupe_preserve_order(links)


def dedupe_preserve_order(iterable):
    seen = set()
    out = []
    for x in iterable:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# ====== CSV APPEND (z nagłówkiem jeśli brak) ======
def append_rows_csv(path: Path, rows: list[dict]):
    new_file = not path.exists()
    with path.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=FIELDS,
            delimiter=";",          # ✅ KLUCZOWE
            quoting=csv.QUOTE_MINIMAL
        )
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})



def count_saved_rows(out_path: Path) -> int:
    """
    Zwraca liczbę JUŻ zapisanych rekordów w pliku wynikowym CSV (bez nagłówka).
    Jeżeli plik nie istnieje lub jest pusty – zwraca 0.
    """
    if not out_path.exists():
        return 0
    try:
        with out_path.open("r", encoding="utf-8-sig", newline="") as fh:
            rd = csv.reader(fh)
            first = next(rd, None)
            if not first:
                return 0
            return sum(1 for _ in rd)
    except Exception:
        return 0


# ====== GŁÓWNA PĘTLA ======
def main():
    ap = argparse.ArgumentParser(
        description="Scraper otodom — tryb B: --input/--output (zgodny też z --links_dir/--out_dir)."
    )
    # Nowy tryb (preferowany)
    ap.add_argument("--input", help="pełna ścieżka do pliku z linkami (CSV lub lista URL-i)", default=None)
    ap.add_argument("--output", help="pełna ścieżka do pliku wynikowego CSV", default=None)

    # Stary tryb (kompatybilność)
    ap.add_argument("--region", help="np. podlaskie, dolnośląskie itd. (tylko ze starym trybem)", default=None)
    ap.add_argument("--links_dir", help="katalog, w którym są pliki z linkami (stary tryb)", default=None)
    ap.add_argument("--out_dir", help="katalog wyjściowy na pliki z danymi (stary tryb)", default=None)

    # Parametry techniczne
    ap.add_argument("--delay_min", type=float, default=4.0, help="minimalne opóźnienie między ogłoszeniami (sek)")
    ap.add_argument("--delay_max", type=float, default=6.0, help="maksymalne opóźnienie między ogłoszeniami (sek)")
    ap.add_argument("--retries", type=int, default=3, help="liczba prób pobrania pojedynczego ogłoszenia")

    args = ap.parse_args()

    # Ustal ścieżki wejścia/wyjścia
    if args.input and args.output:
        input_path = Path(args.input)
        output_path = Path(args.output)
        region_name = guess_region_name_from_path(input_path)
    else:
        # tryb legacy
        if not (args.region and args.links_dir and args.out_dir):
            ap.error("Podaj --input i --output, albo (legacy) --region, --links_dir i --out_dir.")
        region_file = normalize_region_filename(args.region)
        input_path = Path(args.links_dir) / region_file
        output_path = Path(args.out_dir) / region_file
        region_name = Path(region_file).stem

    if not input_path.exists():
        raise SystemExit(f"[ERR] Brak pliku linków: {input_path}")

    # Przygotuj sesję HTTP
    session = requests.Session()
    session.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })

    # Wczytaj listę linków
    links = read_links_any(input_path)
    total_links = len(links)
    print(f"[start] region='{region_name}' links={total_links} input='{input_path}' output='{output_path}'")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # WZNOWIENIE: start od (liczba_zapisanych + 1)
    saved_rows = count_saved_rows(output_path)
    if saved_rows > 0:
        print(f"[resume] wykryto już zapisane rekordy: {saved_rows} — wznowię od {saved_rows + 1}")
    if saved_rows >= total_links:
        print("[done] Wszystkie linki z pliku wejściowego są już przerobione.")
        return

    todo = links[saved_rows:]
    print(f"[todo] do zrobienia: {len(todo)}")

    # Główna pętla — zapis po KAŻDYM ogłoszeniu (append)
    for idx, url in enumerate(todo, 1):
        print(f"[{idx}/{len(todo)}] Pobieram: {url}")
        row = fetch_one(url, session, retries=args.retries)
        skip, reason = should_skip_row(row)
        if skip:
            print(f"    ↳ [SKIP] {reason}", flush=True)
            continue
        append_rows_csv(output_path, [row])

        # pauza losowa (prawdziwie losowa w podanym zakresie)
        if args.delay_max > 0:
            delay = random.uniform(max(0.0, args.delay_min), max(args.delay_min, args.delay_max))
            print(f"    ↳ pauza {delay:.2f} s…")
            time.sleep(delay)

    print(f"[OK] dopisano {len(todo)} rekordów do pliku: {output_path}")


# ====== POMOCNICZE ======
def normalize_region_filename(region: str) -> str:
    """
    Zamienia np. 'dolnoslaskie' / 'dolnośląskie' na nazwę pliku z wielką literą i polskimi znakami
    jeśli plik tak jest nazwany w katalogu z linkami. Domyślnie tworzy <Region>.csv.
    """
    base = region.strip()
    if not base:
        return "Region.csv"
    if base.lower().endswith(".csv"):
        return base
    return f"{base[0].upper()}{base[1:]}.csv"


if __name__ == "__main__":
    main()

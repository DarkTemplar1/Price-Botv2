# -*- coding: utf-8 -*-
"""
adres_otodom.py — wyciąganie woj/pow/gm/miasto/dzielnica/ulica z ogłoszeń Otodom
- preferencja: JSON w <script> (street/city/district/province)
- fallback: DOM (link 'Pokaż na mapie', fragmenty adresu)
- twarde filtry żeby nie łapać tekstów UI (Wróć, Udostępnij, Otomoto.pl itd.)
"""

from __future__ import annotations
import re
from typing import Dict, Optional
from bs4 import BeautifulSoup

# Frazy/elementy interfejsu, których NIGDY nie traktujemy jako części ulicy
_UI_BLACKLIST = {
    # ogólne przyciski/CTA
    "wróć", "wroc", "udostępnij", "udostepnij", "zapisz", "obserwuj",
    "wszystkie zdjęcia", "wszystkie zdjecia", "pokaż na mapie", "pokaz na mapie",
    "zadzwoń", "napisz", "drukuj", "pobierz", "pełny ekran", "pelny ekran",
    "galeria", "wideo", "video", "wirtualny spacer",
    # serwisy grupy
    "otomoto.pl", "fixly.pl", "obido.pl", "kupuję nieruchomości", "kupuje nieruchomosci",
    # nawigacja/mapy
    "google", "maps", "openstreetmap", "wyznacz trasę", "wyznacz trase", "trasa", "dojazd",
}

# Publiczny alias (żeby inne moduły mogły użyć tej listy filtrów)
UI_BLACKLIST = _UI_BLACKLIST

def is_ui_garbage(text: Optional[str]) -> bool:
    """Czy dany tekst wygląda na śmieć z UI / link / domenę (np. 'Wróć', 'Udostępnij', 'otomoto.pl', 'maps')?"""
    t = _clean(text)
    if not t:
        return False
    tl = t.lower()
    if _has_tld(tl):
        return True
    return any(bad in tl for bad in _UI_BLACKLIST)

def should_skip_csv_row(row: Dict[str, str], fields: Optional[list] = None) -> bool:
    """
    Zwraca True jeśli wiersz NIE powinien trafić do CSV, bo zawiera śmieciowe treści z UI.
    Domyślnie sprawdza pola adresowe (jak w parse_address).
    """
    if fields is None:
        fields = ["wojewodztwo", "powiat", "gmina", "miejscowosc", "dzielnica", "ulica"]

    # 1) jeśli jakiekolwiek pole ma śmieci — skip
    for f in fields:
        v = row.get(f, "")
        if is_ui_garbage(v):
            return True

    # 2) jeśli brak kluczowych pól (miejscowość + woj) — zwykle oznacza źle sparsowaną stronę
    if not _clean(row.get("miejscowosc")) and not _clean(row.get("wojewodztwo")):
        return True

    return False

# Prefiksy/typy dróg dopuszczalne na początku nazwy
_STREET_PREFIX = r"(ul\.|ulica|al\.|aleja|alei|aleje|pl\.|plac|os\.|osiedle|rynek|rondo|bulw\.|bulwar|skwer)"

# ------------------------------ utils ----------------------------------------


def _clean(text: Optional[str]) -> str:
    if not text:
        return ""
    t = re.sub(r"\s+", " ", str(text)).strip()
    # odetnij „dopiski” po separatorach widocznych w nagłówkach
    t = re.sub(r"[|•·—–]\s*.*$", "", t)
    return t


def _has_tld(s: str) -> bool:
    """Czy zawiera coś co wygląda na domenę (np. otomoto.pl) albo URL."""
    return bool(re.search(r"(https?://|www\.)|\b[a-z0-9.-]+\.(pl|com|net|org)\b", s, re.I))


def _normalize_street(s: str) -> str:
    """Napraw typowe zlepki z DOM-u: 'ul. .' / 'al. eja' / 'pl. ac' itp."""
    t = s
    # podwójne kropki po prefiksach
    t = re.sub(r"(?i)\b(ul|al|pl)\.\s*\.\s*", r"\1. ", t)
    # 'ul. ica' -> 'ul.'
    t = re.sub(r"(?i)\bul\.\s*lica\b", "ul.", t)
    # 'al. eja' -> 'al.'
    t = re.sub(r"(?i)\bal\.\s*eja\b", "al.", t)
    # 'pl. ac' -> 'pl.'
    t = re.sub(r"(?i)\bpl\.\s*ac\b", "pl.", t)
    # zbędne spacje/kropki
    t = re.sub(r"\s{2,}", " ", t).strip(" ,.-")
    return t


def _looks_like_street(s: str) -> str:
    """
    Zwróć s jeśli wygląda jak wiarygodna nazwa ulicy; w przeciwnym razie pusty string.
    """
    if not s:
        return ""
    t = _normalize_street(_clean(s))
    tl = t.lower()

    # odrzuć domieszki interfejsu, domeny itp.
    if _has_tld(tl):
        return ""
    for bad in _UI_BLACKLIST:
        if bad in tl:
            return ""

    # musi mieć sensowną długość
    words = t.split()
    if len(words) == 0 or len(words) > 8:
        return ""

    # preferuj znany prefiks ulicy
    if re.match(rf"^{_STREET_PREFIX}\b", t, re.I):
        return t

    # bez prefiksu: wygląda jak nazwa własna (wielka litera) i nie jest zdaniem/tytułem
    if re.match(r"^[A-ZĄĆĘŁŃÓŚŻŹ]", t) and not t.endswith(("!", "?", ".")):
        return t

    return ""


def _first(patterns, text: str) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, re.I | re.S)
        if m:
            return _clean(m.group(1))
    return None


def _from_json_scripts(html: str) -> Dict[str, str]:
    """Szybkie parsowanie JSON-ów osadzonych na stronie (bez json.loads)."""
    out = {"province": "", "county": "", "gmina": "", "city": "", "district": "", "street": ""}

    street = _first([
        r'"streetLabel"\s*:\s*"([^"]+)"',
        r'"streetName"\s*:\s*"([^"]+)"',
        r'"street"\s*:\s*"([^"]+)"',
        r'"route"\s*:\s*"([^"]+)"',
    ], html)

    city = _first([
        r'"cityLabel"\s*:\s*"([^"]+)"',
        r'"city"\s*:\s*"([^"]+)"',
        r'"locality"\s*:\s*"([^"]+)"',
    ], html)

    district = _first([
        r'"districtLabel"\s*:\s*"([^"]+)"',
        r'"district"\s*:\s*"([^"]+)"',
        r'"subLocality"\s*:\s*"([^"]+)"',
    ], html)

    province = _first([
        r'"province"\s*:\s*"([^"]+)"',
        r'"voivodeship"\s*:\s*"([^"]+)"',
    ], html)

    out["province"] = _clean(province) if province else ""
    out["city"] = _clean(city) if city else ""
    out["district"] = _clean(district) if district else ""
    out["street"] = _looks_like_street(street) if street else ""
    return out


def _from_dom(soup: BeautifulSoup) -> Dict[str, str]:
    """Fallback: wyciąga elementy z DOM."""
    out = {"province": "", "county": "", "gmina": "", "city": "", "district": "", "street": ""}

    # 1) Pasek adresu: "Miasto, Dzielnica, Województwo"
    head = soup.select_one('[data-cy="adPageHeader-address"]')
    if head:
        addr = _clean(head.get_text(" ", strip=True))
        parts = [p.strip() for p in addr.split(",") if p.strip()]
        if parts:
            out["city"] = parts[0]
        if len(parts) > 1:
            out["district"] = parts[1]
        if len(parts) > 2:
            out["province"] = parts[-1]

    # 2) Link „Pokaż na mapie …”
    mlink = soup.select_one('[data-cy="adPageMap-link"], a[href*="google.com/maps"], a[href*="maps.google"]')
    if mlink:
        raw = _clean(mlink.get_text(" ", strip=True))
        raw = re.sub(r"(?i)^pokaż na mapie\s*", "", raw)
        raw = re.sub(r"(?i)^pokaz na mapie\s*", "", raw)
        raw = raw.split(",")[0].strip()
        s = _looks_like_street(raw)
        if s:
            out["street"] = s

    # 3) Inne etykiety z nazwą ulicy — tylko jeśli zaczną się od prefiksu
    if not out["street"]:
        cand = soup.find(string=re.compile(rf"^{_STREET_PREFIX}\b", re.I))
        if cand:
            s = _looks_like_street(cand)
            if s:
                out["street"] = s

    return out


# ----------------------------- API główne ------------------------------------


def parse_address(html: str) -> Dict[str, str]:
    """
    Zwraca dict:
      wojewodztwo, powiat, gmina, miejscowosc, dzielnica, ulica
    (puste stringi gdy brak). Nigdy nie zwraca śmieci typu „Wróć/Udostępnij…”.
    """
    res = {"wojewodztwo": "", "powiat": "", "gmina": "", "miejscowosc": "", "dzielnica": "", "ulica": ""}

    # 1) JSON (szybko, stabilnie)
    js = _from_json_scripts(html)

    # 2) DOM (uzupełnienie)
    soup = BeautifulSoup(html, "html.parser")
    dm = _from_dom(soup)

    # 3) scalanie — preferuj JSON; DOM tylko jako fill-in
    res["wojewodztwo"] = js["province"] or dm["province"] or ""
    res["miejscowosc"] = js["city"] or dm["city"] or ""
    res["dzielnica"] = js["district"] or dm["district"] or ""

    street = js["street"] or dm["street"] or ""
    res["ulica"] = _looks_like_street(street)

    # dodatkowe bezpieczniki: nie zwracaj śmieci z UI
    for k in ("wojewodztwo", "miejscowosc", "dzielnica", "ulica"):
        if is_ui_garbage(res.get(k, "")):
            res[k] = ""

    # powiat/gmina zostają puste (Otodom rzadko je podaje pewnie)
    res["powiat"] = ""
    res["gmina"] = ""
    return res

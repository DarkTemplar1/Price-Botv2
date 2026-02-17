#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

SHEET_RAPORT = "raport"
SHEET_ODF = "raport_odfiltrowane"
COL_UDZ = "Czy udziały?"

def _load_or_first(xlsx: Path) -> str:
    xl = pd.ExcelFile(xlsx, engine="openpyxl")
    return SHEET_RAPORT if SHEET_RAPORT in xl.sheet_names else xl.sheet_names[0]

def _ensure_odf(xlsx: Path, header_cols: list[str]):
    # jeśli brak – zapisz pusty z samym nagłówkiem
    try:
        pd.read_excel(xlsx, sheet_name=SHEET_ODF, engine="openpyxl")
    except Exception:
        df0 = pd.DataFrame(columns=header_cols)
        with pd.ExcelWriter(xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as wr:
            df0.to_excel(wr, sheet_name=SHEET_ODF, index=False)

def main():
    xlsx = Path(sys.argv[sys.argv.index("--in")+1]).expanduser() if "--in" in sys.argv else None
    if not xlsx or not xlsx.exists():
        print("[ERR] Podaj: --in <plik.xlsx>")
        sys.exit(1)

    src_sheet = _load_or_first(xlsx)
    df = pd.read_excel(xlsx, sheet_name=src_sheet, engine="openpyxl")
    if COL_UDZ not in df.columns:
        print(f"[ERR] Brak kolumny: {COL_UDZ}")
        sys.exit(2)

    # do przerzutu: wszystko co NIE zawiera 'nie'
    mask_move = ~df[COL_UDZ].astype(str).str.contains(r"\bnie\b", case=False, na=False, regex=True)
    to_move = df[mask_move].copy()
    stay = df[~mask_move].copy()

    _ensure_odf(xlsx, list(df.columns))
    try:
        df_odf = pd.read_excel(xlsx, sheet_name=SHEET_ODF, engine="openpyxl")
    except Exception:
        df_odf = pd.DataFrame(columns=df.columns)

    # ujednolić kolumny
    to_move = to_move.reindex(columns=df_odf.columns, fill_value="")

    new_odf = pd.concat([df_odf, to_move], ignore_index=True)
    with pd.ExcelWriter(xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as wr:
        stay.to_excel(wr, sheet_name=src_sheet, index=False)
        new_odf.to_excel(wr, sheet_name=SHEET_ODF, index=False)

    print(f"[OK] Przerzucono: {len(to_move)}  |  Pozostało w '{src_sheet}': {len(stay)}")

if __name__ == "__main__":
    main()

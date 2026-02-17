#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scalanie.py
Scalanie wojewódzkich CSV (separator ;) do Polska.xlsx

- każdy CSV -> osobny arkusz
- arkusz zbiorczy: "Polska YYYY-MM-DD HH-MM"
- pełna zawartość CSV (1:1)
- USUWA rekordy z błędem:
  "ERROR: Nie udało się wyciągnąć kluczowych pól"
"""

from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd


ERROR_MARKER = "ERROR: Nie udało się wyciągnąć kluczowych pól"


# ---------------- CSV ----------------

def read_csv_pl(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=";", encoding="utf-8-sig", engine="python")
    except UnicodeDecodeError:
        return pd.read_csv(path, sep=";", encoding="cp1250", engine="python")
    except Exception:
        return pd.read_csv(path, sep=";", engine="python")


def drop_error_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Usuń wiersze zawierające komunikat ERROR w dowolnej kolumnie."""
    if df.empty:
        return df

    mask = df.astype(str).apply(
        lambda row: row.str.contains(ERROR_MARKER, case=False, na=False).any(),
        axis=1
    )

    removed = int(mask.sum())
    if removed > 0:
        print(f"[INFO] Usunięto {removed} wierszy z ERROR")

    return df[~mask].copy()


# ---------------- Excel helpers ----------------

def safe_sheet_name(name: str) -> str:
    bad = ['[', ']', ':', '*', '?', '/', '\\']
    out = name
    for ch in bad:
        out = out.replace(ch, "_")
    out = out.strip() or "Sheet"
    return out[:31]


# ---------------- MAIN ----------------

def scal_do_excela(base_dir: Path) -> Path:
    base_dir = base_dir.resolve()
    woj_dir = base_dir / "województwa"
    out_xlsx = base_dir / "Polska.xlsx"

    if not woj_dir.exists():
        raise SystemExit(f"[ERR] Brak folderu 'województwa' w: {base_dir}")

    csv_files = sorted(woj_dir.glob("*.csv"))
    if not csv_files:
        raise SystemExit(f"[ERR] Brak plików CSV w: {woj_dir}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H-%M")
    polska_sheet = safe_sheet_name(f"Polska {timestamp}")

    all_dfs = []
    region_sheets = []

    for csv_path in csv_files:
        df = read_csv_pl(csv_path)
        before = len(df)

        df = drop_error_rows(df)

        after = len(df)
        if before != after:
            print(f"[CLEAN] {csv_path.name}: {before} → {after}")

        sheet_name = safe_sheet_name(csv_path.stem)
        region_sheets.append((sheet_name, df))
        all_dfs.append(df)

    df_all = pd.concat(all_dfs, ignore_index=True)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for sheet_name, df in region_sheets:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        df_all.to_excel(writer, sheet_name=polska_sheet, index=False)

        info = pd.DataFrame({
            "generated_at": [datetime.now().isoformat(timespec="seconds")],
            "source_folder": [str(woj_dir)],
            "num_files": [len(csv_files)],
            "total_rows": [len(df_all)],
            "polska_sheet": [polska_sheet],
        })
        info.to_excel(writer, sheet_name="INFO", index=False)

    print(f"[OK] Zapisano {out_xlsx}")
    print(f"[OK] Arkusz zbiorczy: {polska_sheet}")
    return out_xlsx


def main():
    parser = argparse.ArgumentParser(description="Scal województwa/*.csv do Polska.xlsx")
    parser.add_argument(
        "--base",
        default=None,
        help="Folder bazowy (tam gdzie jest folder 'województwa')"
    )
    args = parser.parse_args()

    base_dir = Path(args.base) if args.base else Path.cwd()
    scal_do_excela(base_dir)


if __name__ == "__main__":
    main()

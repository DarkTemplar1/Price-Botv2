#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import csv
import os
import signal
import sys
import time
import threading
import subprocess
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import unicodedata
import re

# ===== konfiguracja =====
VOIVODESHIPS = [
    "Dolnośląskie","Kujawsko-Pomorskie","Lubelskie","Lubuskie","Łódzkie","Małopolskie",
    "Mazowieckie","Opolskie","Podkarpackie","Podlaskie","Pomorskie","Śląskie",
    "Świętokrzyskie","Warmińsko-Mazurskie","Wielkopolskie","Zachodniopomorskie",
]
DELAY_MIN = 4.0
DELAY_MAX = 6.0
RETRIES   = 3
SOFT_STOP_MORE = 10   # ile ogłoszeń „dokończyć” po kliknięciu Zatrzymaj

IS_FROZEN = getattr(sys, "frozen", False)


# ============================ utils ============================

def _normalize_region_slug(name: str) -> str:
    """np. 'Warmińsko-Mazurskie' -> 'warminsko-mazurskie' (slug dla --region)"""
    s = (name or "").strip().lower()
    # KLUCZ: 'ł' nie rozkłada się w NFKD -> zamieniamy ręcznie
    s = s.replace("ł", "l").replace("Ł", "l")
    # usuń diakrytyki z reszty
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    # spacje -> '-'
    s = re.sub(r"\s+", "-", s)
    # tylko a-z0-9-
    s = re.sub(r"[^a-z0-9\-]+", "", s)
    # zredukuj wielokrotne myślniki do jednego
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def _raise_in_thread(thread: threading.Thread, exctype=SystemExit) -> bool:
    """Wstrzykuj wyjątek do wskazanego wątku (łagodne wyjście)."""
    ident = getattr(thread, "ident", None)
    if ident is None:
        return False
    import ctypes
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(ident), ctypes.py_object(exctype))
    if res > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(ident), None)
        return False
    return res == 1


# ============================ OKNO BAZY DANYCH ============================

class BazaDanychWindow(tk.Toplevel):
    """
    Okno „Baza danych” (uruchamiane z selektor_csv.py lub standalone).
    """
    def __init__(self, parent: tk.Misc | None, base_dir: Path, standalone: bool = False):
        master = parent if parent is not None else (tk.Tk() if standalone else tk.Tk())
        super().__init__(master=master) if parent is not None else super().__init__(master=master)
        self.standalone = standalone and parent is None
        self.title("PriceBot — Baza danych")
        self.minsize(900, 540)

        # ścieżki
        self.base_dir  = Path(base_dir).expanduser().resolve()
        self.links_dir = self.base_dir / "linki"
        self.out_dir   = self.base_dir / "województwa"
        self.timing_csv = self.base_dir / "timing.csv"
        self.logs_dir  = self.base_dir / "logs"

        # uruchomienia / etapy
        self.proc_by_region: dict[str, subprocess.Popen] = {}
        self.thread_by_region: dict[str, threading.Thread] = {}
        self.stage_by_region: dict[str, str] = {}  # 'links' | 'ads'
        self.active_region: str | None = None      # jedyny aktywny region

        # miękkie zatrzymanie
        self.soft_stop_targets: dict[str, int] = {}                # region -> docelowa liczba wierszy
        self.soft_stop_monitors: dict[str, threading.Thread] = {}  # region -> wątek monitorujący

        # sterowanie UI
        self._lock_start_until_stop = False
        self._suspend_select_events = False  # nie wywołuj handlerów select podczas odświeżania

        self._ensure_minimal_structure()
        self._build_ui()
        self.refresh()
        self.after(2000, self._auto_refresh)
        self._update_start_button_state()

        if self.standalone:
            self.protocol("WM_DELETE_WINDOW", self._on_close_standalone)
            self.master.withdraw()
            self.deiconify()

    # ---------- struktura ----------
    def _ensure_minimal_structure(self):
        self.links_dir.mkdir(parents=True, exist_ok=True)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        if not self.timing_csv.exists():
            with self.timing_csv.open("w", encoding="utf-8-sig", newline="") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "region","phase","status","processed","total","updated_at",
                    "delay_min","delay_max"
                ])
                w.writeheader()

    # ---------- UI ----------
    def _build_ui(self):
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)

        # Pasek przycisków
        bar = ttk.Frame(root)
        bar.pack(fill="x")

        self.btn_start = ttk.Button(bar, text="Start/Wznów", command=self.on_start)
        self.btn_start.pack(side="left")

        # --- przycisk Zatrzymaj z kolorami jak Automat ---
        self.btn_stop = tk.Button(
            bar,
            text="Zatrzymaj",
            command=self.on_stop,
            bg="#d9d9d9",
            activebackground="#d0d0d0",
        )
        self.btn_stop.pack(side="left", padx=(6, 0))

        # --- przycisk Scal do Polska.xlsx z kolorami ---
        self.btn_scal = tk.Button(
            bar,
            text="Scal do Polska.xlsx",
            command=self.run_scalanie,
            bg="#d9d9d9",
            activebackground="#d0d0d0",
        )
        self.btn_scal.pack(side="left", padx=(6, 0))

        ttk.Label(bar, text="  Baza:").pack(side="left", padx=(12, 4))
        self.base_var = tk.StringVar(value=str(self.base_dir))
        ttk.Entry(bar, textvariable=self.base_var, width=60).pack(side="left", fill="x", expand=True)
        ttk.Button(bar, text="Zmień…", command=self._pick_base).pack(side="left", padx=(6, 0))

        # Drzewko statusów
        tree_box = ttk.Frame(root)
        tree_box.pack(fill="both", expand=False, pady=(10, 6))
        cols = ("region","phase","status","progress","pct","updated")
        self.tree = ttk.Treeview(tree_box, columns=cols, show="headings", height=12)
        for c, txt, w in [
            ("region","Województwo",180),
            ("phase","Faza",90),
            ("status","Status",140),
            ("progress","Postęp",110),
            ("pct","%",60),
            ("updated","Aktualizacja",160),
        ]:
            self.tree.heading(c, text=txt); self.tree.column(c, width=w, anchor="w", stretch=(c=="region"))
        self.tree.pack(side="left", fill="both", expand=True)
        sc = ttk.Scrollbar(tree_box, orient="vertical", command=self.tree.yview)
        sc.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=sc.set)
        self.tree.bind("<<TreeviewSelect>>", self._on_select_iid)


    # ---------- pick base ----------
    def _pick_base(self):
        d = filedialog.askdirectory(title="Folder bazowy (z linki/ i województwa/)", initialdir=str(self.base_dir))
        if not d:
            return
        self.base_dir = Path(d).resolve()
        self.links_dir = self.base_dir / "linki"
        self.out_dir   = self.base_dir / "województwa"
        self.logs_dir  = self.base_dir / "logs"
        self.timing_csv = self.base_dir / "timing.csv"
        self._ensure_minimal_structure()
        self.refresh()

    # ---------- odczyty pomocnicze ----------
    def _read_links_count(self, f: Path) -> int:
        if not f.exists(): return 0
        try:
            with f.open("r", encoding="utf-8-sig", newline="") as fh:
                # 1 kolumna, nagłówek 'link'
                return max(0, sum(1 for _ in fh) - 1)
        except Exception:
            return 0

    def _read_processed_count(self, f: Path) -> int:
        if not f.exists(): return 0
        try:
            with f.open("r", encoding="utf-8-sig", newline="") as fh:
                # CSV z nagłówkiem – licz wiersze danych
                rd = csv.reader(fh)
                first = True
                n = 0
                for row in rd:
                    if first:
                        first = False
                        continue
                    if row and any(c.strip() for c in row):
                        n += 1
                return n
        except Exception:
            return 0

    def _load_timing(self) -> dict[str, dict]:
        log: dict[str, dict] = {}
        if not self.timing_csv.exists():
            return log
        with self.timing_csv.open("r", encoding="utf-8-sig", newline="") as f:
            rd = csv.DictReader(f)
            for r in rd:
                log[r.get("region","")] = r
        return log

    def _save_timing_row(self, region: str, phase: str, status: str, processed: int, total: int):
        rows = self._load_timing()
        rows[region] = {
            "region": region,
            "phase": phase,
            "status": status,
            "processed": str(processed),
            "total": str(total),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "delay_min": str(DELAY_MIN),
            "delay_max": str(DELAY_MAX),
        }
        with self.timing_csv.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["region","phase","status","processed","total","updated_at","delay_min","delay_max"])
            w.writeheader()
            for _, r in rows.items():
                w.writerow(r)

    # ---------- wybór ----------
    def _on_select_iid(self, _evt=None):
        if self._suspend_select_events:
            return

    def _selected_region(self) -> str | None:
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Baza danych", "Zaznacz województwo na liście.")
            return None
        iid = sel[0]
        return self.tree.set(iid, "region") or iid

    def _any_running(self) -> str | None:
        if self.active_region is not None:
            t = self.thread_by_region.get(self.active_region)
            p = self.proc_by_region.get(self.active_region)
            if (t and t.is_alive()) or (p and p.poll() is None):
                return self.active_region
            else:
                self.active_region = None
        for r, p in self.proc_by_region.items():
            if p and p.poll() is None: return r
        for r, t in self.thread_by_region.items():
            if t and t.is_alive(): return r
        return None

    def _update_start_button_state(self):
        if hasattr(self, "btn_start"):
            disabled = self._any_running() or self._lock_start_until_stop
            self.btn_start.config(state=("disabled" if disabled else "normal"))

    # ---------- odświeżanie ----------
    def refresh(self):
        prev_sel = tuple(self.tree.selection())
        prev_focus = self.tree.focus() if self.tree.focus() else (prev_sel[0] if prev_sel else "")
        prev_yview = self.tree.yview()

        self.tree.delete(*self.tree.get_children())
        log = self._load_timing()

        for region in VOIVODESHIPS:
            lf = self.links_dir / f"{region}.csv"
            of = self.out_dir   / f"{region}.csv"
            total = self._read_links_count(lf)
            done  = self._read_processed_count(of)
            if region in log:
                try: total = max(total, int(log[region].get("total") or 0))
                except ValueError: pass
            pct = f"{(done/total*100):.1f}%" if total else "-"
            phase = log.get(region, {}).get("phase") or ("links" if total == 0 else "ads")
            status = log.get(region, {}).get("status") or "Brak/Stop"
            updated = log.get(region, {}).get("updated_at") or "-"

            if region in self.soft_stop_targets:
                status = "Kończenie (+10)…"

            if (region in self.proc_by_region and self.proc_by_region[region].poll() is None) \
               or (region in self.thread_by_region and self.thread_by_region[region].is_alive()):
                status = "W trakcie"

            self.tree.insert("", "end", iid=region,
                             values=(region, phase, status, f"{done}/{total}", pct, updated))

        try:
            if prev_focus and self.tree.exists(prev_focus):
                self.tree.focus(prev_focus); self.tree.selection_set(prev_focus); self.tree.see(prev_focus)
            elif prev_sel:
                for iid in prev_sel:
                    if self.tree.exists(iid):
                        self.tree.selection_set(iid); self.tree.focus(iid); self.tree.see(iid); break
        except Exception: pass
        try: self.tree.yview_moveto(prev_yview[0])
        except Exception: pass

    def _safe_refresh(self):
        self._suspend_select_events = True
        try: self.refresh()
        finally: self._suspend_select_events = False

    # ---------- start/stop ----------
    def on_start(self):
        if self._any_running():
            return
        region = self._selected_region()
        if not region:
            return

        lf = self.links_dir / f"{region}.csv"
        of = self.out_dir   / f"{region}.csv"

        done_marker = lf.with_suffix(".done")
        stop_marker = lf.with_suffix(".stop")

        total = self._read_links_count(lf)
        done  = self._read_processed_count(of)

        # przy starcie reset stop na SZARY
        try:
            self.btn_stop.config(bg="#d9d9d9", activebackground="#d0d0d0")
        except Exception:
            pass

        # jeśli ktoś skasował CSV, ale został .done → posprzątaj
        if total == 0 and done_marker.exists():
            try:
                done_marker.unlink()
            except Exception:
                pass

        # LINKI są "gotowe" dopiero gdy istnieje marker .done
        links_ready = (total > 0) and done_marker.exists()

        if not links_ready:
            # najpierw LINKI (start / wznowienie)
            # usuń ewentualny STOP, żeby wznowienie nie zatrzymało się od razu
            try:
                if stop_marker.exists():
                    stop_marker.unlink()
            except Exception:
                pass

            self._save_timing_row(region, "links", "W trakcie", done, total)
            if (self.master is not None and not isinstance(self.master, tk.Tk)) or IS_FROZEN:
                self._run_links_threaded(region, lf)
            else:
                script = Path(__file__).with_name("linki_mieszkania.py")
                if not script.exists():
                    messagebox.showerror("Baza danych", f"Nie znaleziono pliku: {script}"); return
                cmd = [sys.executable, str(script), "--region", _normalize_region_slug(region), "--output", str(lf)]
                self._dev_subprocess(cmd, region, stage="links")
        else:
            # bezpośrednio ADS (scraper)
            self._start_ads_for(region)

        self._lock_start_until_stop = True
        self._update_start_button_state()
        self._safe_refresh()


    def on_stop(self):
        region = self._selected_region()
        if not region:
            return

        th = self.thread_by_region.get(region)
        pr = self.proc_by_region.get(region)
        stage = self.stage_by_region.get(region)

        lf = self.links_dir / f"{region}.csv"
        stop_marker = lf.with_suffix(".stop")

        running = (th and th.is_alive()) or (pr and pr.poll() is None)

        # jeśli coś działa – zatrzymaj zależnie od etapu
        if running:
            # 1) LINKI: sygnał STOP przez plik *.stop (skrypt przerwie po bieżącej stronie)
            if stage == "links":
                try:
                    stop_marker.parent.mkdir(parents=True, exist_ok=True)
                    stop_marker.write_text(datetime.now().isoformat(), encoding="utf-8")
                except Exception:
                    # nawet jeśli nie uda się zapisać, nie wywalaj GUI
                    pass

                # ŻÓŁTY = stop w toku
                try:
                    self.btn_stop.config(bg="#f7e26b", activebackground="#f5d742")
                except Exception:
                    pass

                messagebox.showinfo(
                    "Zatrzymanie",
                    "Wysyłam STOP dla pobierania linków…\n"
                    "Zatrzyma się po dokończeniu bieżącej strony i zapisze zebrane linki."
                )
                self._lock_start_until_stop = True
                self._update_start_button_state()
                return

            # 2) ADS: miękki stop (dokończ jeszcze kilka ogłoszeń)
            try:
                self.btn_stop.config(bg="#f7e26b", activebackground="#f5d742")
            except Exception:
                pass
            messagebox.showinfo(
                "Zatrzymanie",
                f"Wstrzymuję po ukończeniu jeszcze {SOFT_STOP_MORE} ogłoszeń…\n"
                "Po osiągnięciu limitu zadanie zatrzyma się i będzie czekało na wznowienie."
            )
            self._start_soft_stop_monitor(region)
            self._lock_start_until_stop = True
            self._update_start_button_state()
            return

        # nic nie działa – zwykłe zatrzymanie stanu + reset koloru
        lf = self.links_dir / f"{region}.csv"; of = self.out_dir / f"{region}.csv"
        total = self._read_links_count(lf); done = self._read_processed_count(of)

        self.proc_by_region.pop(region, None)
        self.thread_by_region.pop(region, None)
        self.stage_by_region.pop(region, None)
        self.active_region = None
        self.soft_stop_targets.pop(region, None)

        phase = "links" if total == 0 else "ads"
        self._save_timing_row(region, phase, "Stop", done, total)
        self._lock_start_until_stop = False
        self._safe_refresh()
        self._update_start_button_state()
        try:
            self.btn_stop.config(bg="#d9d9d9", activebackground="#d0d0d0")
        except Exception:
            pass


    # ---------- uruchamianie (wątek – EXE/Toplevel) ----------
    def _run_links_threaded(self, region: str, lf: Path):
        def worker(_region: str, _lf: Path):
            try:
                import linki_mieszkania as lm
            except Exception as e:
                print(f"Import linki_mieszkania nieudany: {e}", file=sys.stderr)
                messagebox.showerror("Baza danych", f"Import linki_mieszkania nieudany:\n{e}")
                return
            self.stage_by_region[_region] = "links"
            old_argv = sys.argv
            sys.argv = ["linki_mieszkania.py", "--region", _normalize_region_slug(_region), "--output", str(_lf)]
            try:
                lm.main()
            except SystemExit:
                pass
            except Exception as e:
                print(f"Błąd w linki_mieszkania: {e}", file=sys.stderr)
                messagebox.showerror("Baza danych", f"Błąd w linki_mieszkania:\n{e}")
            finally:
                sys.argv = old_argv

        th = threading.Thread(target=worker, args=(region, lf), daemon=True)
        self.thread_by_region[region] = th
        th.start()
        self.active_region = region

    def _run_ads_threaded(self, region: str, lf: Path, of: Path):
        def worker(_region: str, _lf: Path, _of: Path):
            try:
                import scraper_otodom_mieszkania as scraper
            except Exception as e:
                print(f"Import scrapera nieudany: {e}", file=sys.stderr)
                messagebox.showerror("Baza danych", f"Import scrapera nieudany:\n{e}")
                return
            self.stage_by_region[_region] = "ads"
            old_argv = sys.argv
            sys.argv = [
                "scraper_otodom_mieszkania.py","--input",str(_lf),"--output",str(_of),
                "--delay_min",str(DELAY_MIN),"--delay_max",str(DELAY_MAX),"--retries",str(RETRIES)
            ]
            try:
                scraper.main()
            except SystemExit:
                pass
            except Exception as e:
                print(f"Błąd w scraperze: {e}", file=sys.stderr)
                messagebox.showerror("Baza danych", f"Błąd w scraperze:\n{e}")
            finally:
                sys.argv = old_argv

        th = threading.Thread(target=worker, args=(region, lf, of), daemon=True)
        self.thread_by_region[region] = th
        th.start()
        self.active_region = region

    # ---------- uruchamianie (dev subprocess – z podglądem w terminalu) ----------
    def _dev_subprocess(self, cmd: list[str], region: str, stage: str):
        try:
            # Wymuś UTF-8 w dziecku (na wszelki wypadek)
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"

            proc = subprocess.Popen(
                cmd,
                cwd=str(Path(__file__).parent),
                stdout=None,                        # output to console
                stderr=None,
                bufsize=0,                           # płynny odczyt w binarnym trybie
                universal_newlines=False,            # NIE dekoduj automatycznie (cp1250 itp.)
                close_fds=os.name != "nt",
                creationflags=0,
                env=env
            )
            self.proc_by_region[region] = proc
            self.stage_by_region[region] = stage
            self.active_region = region
        except Exception as e:
            print(f"Nie udało się uruchomić procesu: {e}", file=sys.stderr)
            messagebox.showerror("Baza danych", f"Nie udało się uruchomić procesu:\n{e}")

    # ---------- auto start ADS po LINKS ----------
    def _start_ads_for(self, region: str):
        if self._any_running() or (self.active_region and self.active_region != region):
            return
        lf = self.links_dir / f"{region}.csv"
        done_marker = lf.with_suffix(".done")
        if not done_marker.exists():
            # Linki nie są kompletne → nie startuj ADS
            return

        of = self.out_dir   / f"{region}.csv"
        total = self._read_links_count(lf); done = self._read_processed_count(of)
        if total == 0:
            return

        self._save_timing_row(region, "ads", "W trakcie", done, total)
        if (self.master is not None and not isinstance(self.master, tk.Tk)) or IS_FROZEN:
            self._run_ads_threaded(region, lf, of)
        else:
            script = Path(__file__).with_name("scraper_otodom_mieszkania.py")
            if not script.exists():
                messagebox.showerror("Baza danych", f"Nie znaleziono pliku: {script}"); return
            cmd = [sys.executable, str(script), "--input", str(lf), "--output", str(of),
                   "--delay_min", str(DELAY_MIN), "--delay_max", str(DELAY_MAX), "--retries", str(RETRIES)]
            self._dev_subprocess(cmd, region, stage="ads")
        self._lock_start_until_stop = True
        self._update_start_button_state()
        self._safe_refresh()


    def _start_soft_stop_monitor(self, region: str):
        """Po kliknięciu 'Zatrzymaj' monitoruje plik i po +SOFT_STOP_MORE wierszach kończy scraper."""
        if region in self.soft_stop_monitors and self.soft_stop_monitors[region].is_alive():
            return

        lf = self.links_dir / f"{region}.csv"
        of = self.out_dir   / f"{region}.csv"
        baseline = self._read_processed_count(of)
        target = baseline + SOFT_STOP_MORE
        self.soft_stop_targets[region] = target

        def monitor():
            try:
                while True:
                    time.sleep(1.0)
                    th = self.thread_by_region.get(region)
                    pr = self.proc_by_region.get(region)
                    alive = (th and th.is_alive()) or (pr and pr.poll() is None)
                    if not alive:
                        break

                    done = self._read_processed_count(of)

                    # DEV: proces — ubij po limicie
                    if pr and pr.poll() is None and done >= target:
                        try:
                            if os.name == "nt":
                                pr.send_signal(signal.CTRL_BREAK_EVENT)
                                time.sleep(0.4)
                            pr.terminate()
                            time.sleep(0.6)
                            if pr.poll() is None:
                                pr.kill()
                        except Exception:
                            pass
                        break

                    # EXE/Toplevel: wątek — wstrzyknij SystemExit
                    if th and th.is_alive() and done >= target:
                        _raise_in_thread(th, SystemExit)
                        break
            finally:
                # porządki i zapis stanu
                self.proc_by_region.pop(region, None)
                self.thread_by_region.pop(region, None)
                self.stage_by_region.pop(region, None)
                self.soft_stop_targets.pop(region, None)
                total_links = self._read_links_count(lf)
                done_now    = self._read_processed_count(of)
                phase = "ads" if total_links > 0 else "links"
                self._save_timing_row(region, phase, "Stop", done_now, total_links)
                self.active_region = None
                self._lock_start_until_stop = False
                self._safe_refresh()
                self._update_start_button_state()
                # ZIELONY = zakończono miękki stop
                try:
                    self.btn_stop.config(bg="#8ef98e", activebackground="#76e476")
                except Exception:
                    pass

        t = threading.Thread(target=monitor, daemon=True)
        self.soft_stop_monitors[region] = t
        t.start()

    # ---------- AUTO-REFRESH ----------
    def _auto_refresh(self):
        changed = False

        def _maybe_unlock():
            # jeśli NIC nie działa – odblokuj Start/Wznów
            if not self._any_running():
                self._lock_start_until_stop = False
                self._update_start_button_state()

        # DEV: procesy
        for region, proc in list(self.proc_by_region.items()):
            alive = proc.poll() is None
            lf = self.links_dir / f"{region}.csv"
            of = self.out_dir   / f"{region}.csv"
            total = self._read_links_count(lf)
            done = self._read_processed_count(of)
            stage = self.stage_by_region.get(region)
            done_marker = lf.with_suffix(".done")

            if not alive:
                # proces zakończony
                if stage == "links":
                    self.proc_by_region.pop(region, None); self.stage_by_region.pop(region, None)
                    self._save_timing_row(region, "links", "Stop", done, total); changed = True
                    self.active_region = None

                    # Auto-start ADS tylko jeśli linki zostały ukończone (.done)
                    if done_marker.exists() and total > 0:
                        self._start_ads_for(region)
                        continue

                    _maybe_unlock()
                    continue

                # ADS (lub brak stage)
                self._save_timing_row(region, "ads", "Stop", done, total)
                self.proc_by_region.pop(region, None); self.stage_by_region.pop(region, None)
                self.active_region = None
                changed = True
                _maybe_unlock()

            else:
                cur_phase = stage or ("ads" if done_marker.exists() else "links")
                self._save_timing_row(region, cur_phase, "W trakcie", done, total); changed = True

        # WĄTKI
        for region, th in list(self.thread_by_region.items()):
            lf = self.links_dir / f"{region}.csv"
            of = self.out_dir   / f"{region}.csv"
            total = self._read_links_count(lf)
            done = self._read_processed_count(of)
            stage = self.stage_by_region.get(region)
            done_marker = lf.with_suffix(".done")

            if not th.is_alive():
                # wątek zakończony
                if stage == "links":
                    self.thread_by_region.pop(region, None); self.stage_by_region.pop(region, None)
                    self._save_timing_row(region, "links", "Stop", done, total); changed = True
                    self.active_region = None

                    if done_marker.exists() and total > 0:
                        self._start_ads_for(region)
                        continue

                    _maybe_unlock()
                    continue

                self._save_timing_row(region, "ads", "Stop", done, total)
                self.thread_by_region.pop(region, None); self.stage_by_region.pop(region, None)
                self.active_region = None
                changed = True
                _maybe_unlock()

            else:
                cur_phase = stage or ("ads" if done_marker.exists() else "links")
                self._save_timing_row(region, cur_phase, "W trakcie", done, total); changed = True

        if changed:
            self._safe_refresh()

        _maybe_unlock()
        self.after(2000, self._auto_refresh)


    # ---------- scalanie ----------
    def run_scalanie(self):
        """
        Scala wszystkie CSV z katalogu 'województwa' w self.base_dir
        do pliku Polska.xlsx, używając scalanie.scal_do_excela(self.base_dir).
        """
        xlsx_path = self.base_dir / "Polska.xlsx"

        # ŻÓŁTY = scalanie w toku
        try:
            self.btn_scal.config(bg="#f7e26b", activebackground="#f5d742")
        except Exception:
            pass

        def worker():
            # import scalanie
            try:
                import scalanie as _scal
            except Exception as e:
                def on_import_err():
                    try:
                        self.btn_scal.config(bg="#f28b82", activebackground="#ea4335")
                    except Exception:
                        pass
                    messagebox.showerror("Scalanie", f"Nie można zaimportować scalanie.py:\n{e}")
                self.after(0, on_import_err)
                return

            # sprawdź, czy istnieje folder 'województwa'
            woj_dir = self.base_dir / "województwa"
            if not woj_dir.exists():
                def on_no_woj():
                    try:
                        self.btn_scal.config(bg="#f28b82", activebackground="#ea4335")
                    except Exception:
                        pass
                    messagebox.showerror(
                        "Scalanie",
                        f"Nie znaleziono folderu 'województwa' w:\n{self.base_dir}"
                    )
                self.after(0, on_no_woj)
                return

            # właściwe scalanie
            try:
                _scal.scal_do_excela(self.base_dir)
            except SystemExit as e:
                code = getattr(e, "code", 0)
                if code not in (0, None):
                    def on_sys_exit():
                        try:
                            self.btn_scal.config(bg="#f28b82", activebackground="#ea4335")
                        except Exception:
                            pass
                        messagebox.showerror(
                            "Scalanie",
                            f"Scalanie zakończyło się kodem błędu: {code}"
                        )
                    self.after(0, on_sys_exit)
                    return
            except Exception as err:

                def on_err(err=err):
                    try:
                        self.btn_scal.config(bg="#f28b82", activebackground="#ea4335")
                    except Exception:
                        pass
                    messagebox.showerror("Scalanie", f"Błąd scalania:\n{err}")

                self.after(0, on_err)
                return

            # SUKCES
            def on_done():
                try:
                    self.btn_scal.config(bg="#8ef98e", activebackground="#76e476")
                except Exception:
                    pass
                msg = "Zakończono scalanie do Polska.xlsx."
                if not xlsx_path.exists():
                    msg += "\n(Uwaga: nie widzę pliku wynikowego w bazie.)"
                else:
                    msg += f"\nPlik zapisano jako:\n{xlsx_path}"
                messagebox.showinfo("Scalanie", msg)

            self.after(0, on_done)

        threading.Thread(target=worker, daemon=True).start()

    # ---------- zamknięcie standalone ----------
    def _on_close_standalone(self):
        self.destroy()
        if isinstance(self.master, tk.Tk):
            self.master.destroy()


# ============================ API dla selektor_csv.py ============================

def open_ui(root_dir: Path | str, parent: tk.Misc | None = None):
    """
    Otwiera okno „Baza danych”.
    """
    base = Path(root_dir).resolve()
    if parent is not None:
        win = BazaDanychWindow(parent, base, standalone=False)
        win.transient(parent); win.grab_set()
        parent.wait_window(win); return
    win = BazaDanychWindow(None, base, standalone=True)
    win.mainloop()


# ============================ CLI ============================

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", help="folder bazowy (z podfolderami linki/ i województwa/)", default=None)
    args = ap.parse_args()
    base = Path(args.base) if args.base else Path.cwd()
    open_ui(base, parent=None)
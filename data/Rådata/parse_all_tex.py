#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parser alle .txt-filer i den aktuelle mappe:
- Finder par af <td ...>venstre</td> + <td ...>højre</td> (case-insensitivt, DOTALL).
- Rydder HTML-tags i celler, unescaper entities (&amp; -> &), normaliserer whitespace.
- Konverterer højre celle til heltal (fjerner “støj”; tillader minus).
- Skriver resultatet som <navn>_parsed.csv (semikolon, UTF-8 med BOM).

Valgfri filtre/indstillinger (se 'Brug' nedenfor):
  --drop-sp       : dropper rækker hvor artsnavn indeholder 'sp.'
  --drop-hybrid   : dropper rækker hvor artsnavn indeholder 'hybrid'
  --out-delim     : vælg andet output-delimiter (default ';')
  --suffix        : vælg andet suffix for outputfil (default '_parsed')
  --combine       : ud over enkeltfiler gemmes også én samlet fil 'ALL_combined.csv'

Eksempel:
    python parse_all_txt.py --drop-sp --drop-hybrid --combine
"""

import argparse
import csv
import html as htmlmod
import re
from pathlib import Path
from typing import List, Tuple, Optional

# Regex til at finde par af <td>...</td><td ...>...</td> uanset attributter
TD_PAIR_RE = re.compile(
    r"<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>",
    re.IGNORECASE | re.DOTALL
)

# Fjern alle tags i en celle
TAG_RE = re.compile(r"<[^>]+>")

def clean_cell(s: str) -> str:
    """Fjerner HTML-tags, unescaper entities, normaliserer mellemrum."""
    s = TAG_RE.sub("", s)
    s = htmlmod.unescape(s)
    s = s.replace("\t", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def to_int_maybe(s: str) -> Optional[int]:
    """Konverterer til heltal hvis muligt (fjerner 'støj', beholder minus).
       Returnerer None hvis int ikke kan udtrækkes."""
    if s is None:
        return None
    # hvis tal evt. har tusindtalsseparatorer/whitespace
    s_simple = re.sub(r"[ .,\u00A0]", "", s)  # fjern mellemrum, punktum, komma, NBSP
    # forsøg direkte
    if re.fullmatch(r"-?\d+", s_simple):
        return int(s_simple)
    # fallback: find første heltal
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else None

def parse_txt_content(content: str) -> List[Tuple[str, Optional[int]]]:
    """Returnerer liste af (artsnavn, bemaerk_antal) fra rå HTML/tekst."""
    rows: List[Tuple[str, Optional[int]]] = []
    for left, right in TD_PAIR_RE.findall(content):
        name = clean_cell(left)
        cnt = to_int_maybe(clean_cell(right))
        if name:
            rows.append((name, cnt))
    return rows

def process_txt_file(
    path: Path, out_delim: str, suffix: str,
    drop_sp: bool, drop_hybrid: bool, verbose: bool
) -> Optional[Path]:
    """Parser én .txt-fil og skriver <stem>_parsed.csv."""
    # Læs fil med UTF-8; fallback latin-1
    try:
        content = path.read_text(encoding="utf-8", errors="strict")
    except UnicodeDecodeError:
        content = path.read_text(encoding="latin-1", errors="ignore")

    rows = parse_txt_content(content)

    # Filtrering
    if drop_sp:
        rows = [r for r in rows if not re.search(r"\bsp\.\b", r[0], flags=re.IGNORECASE)]
    if drop_hybrid:
        rows = [r for r in rows if not re.search(r"\bhybrid\b", r[0], flags=re.IGNORECASE)]

    if not rows:
        if verbose:
            print(f"[ADVARSEL] Ingen rækker fundet i {path.name}.")
        # Vi skriver stadig en fil med header for konsistens
        out_path = path.with_name(path.stem + suffix + ".csv")
        with out_path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=out_delim)
            w.writerow(["artsnavn", "bemaerk_antal"])
        return out_path

    out_path = path.with_name(path.stem + suffix + ".csv")
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=out_delim)
        w.writerow(["artsnavn", "bemaerk_antal"])
        for name, cnt in rows:
            w.writerow([name, cnt if cnt is not None else ""])

    if verbose:
        filled = sum(1 for _, c in rows if c is not None)
        print(f"[OK] {path.name} → {out_path.name} | rækker: {len(rows)}, antal udfyldt: {filled}")
    return out_path

def main():
    ap = argparse.ArgumentParser(description="Parser alle .txt-filer i nuværende mappe til artsnavn/bemaerk_antal.")
    ap.add_argument("--out-delim", default=";", help="Output-delimiter (default: ';')")
    ap.add_argument("--suffix", default="_parsed", help="Suffix for outputfiler (default: _parsed)")
    ap.add_argument("--drop-sp", action="store_true", help="Drop rækker hvor artsnavn indeholder 'sp.'")
    ap.add_argument("--drop-hybrid", action="store_true", help="Drop rækker hvor artsnavn indeholder 'hybrid'")
    ap.add_argument("--combine", action="store_true", help="Skriv også en samlet CSV for alle filer (ALL_combined.csv)")
    ap.add_argument("-q", "--quiet", action="store_true", help="Mindsk output")
    args = ap.parse_args()

    txt_files = sorted(Path(".").glob("*.txt"))
    if not txt_files:
        print("Ingen .txt-filer fundet i denne mappe.")
        return

    verbose = not args.quiet
    combined: List[Tuple[str, str, Optional[int]]] = []  # (kilde, artsnavn, antal)

    for f in txt_files:
        out_path = process_txt_file(
            f,
            out_delim=args.out_delim,
            suffix=args.suffix,
            drop_sp=args.drop_sp,
            drop_hybrid=args.drop_hybrid,
            verbose=verbose,
        )
        if args.combine and out_path is not None:
            # læs det vi lige skrev (sikrer ensartethed)
            try:
                with out_path.open("r", encoding="utf-8-sig", newline="") as rf:
                    rdr = csv.DictReader(rf, delimiter=args.out_delim)
                    for row in rdr:
                        name = (row.get("artsnavn") or "").strip()
                        raw_cnt = (row.get("bemaerk_antal") or "").strip()
                        cnt = to_int_maybe(raw_cnt) if raw_cnt != "" else None
                        if name:
                            combined.append((f.name, name, cnt))
            except Exception as e:
                if verbose:
                    print(f"[ADVARSEL] Kunne ikke tilføje {out_path.name} til samlet output: {e}")

    if args.combine and combined:
        comb_path = Path("ALL_combined.csv")
        with comb_path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=args.out_delim)
            w.writerow(["kildefil", "artsnavn", "bemaerk_antal"])
            for src, name, cnt in combined:
                w.writerow([src, name, cnt if cnt is not None else ""])
        if verbose:
            print(f"[OK] Samlet fil skrevet: {comb_path.name} ({len(combined)} rækker)")

if __name__ == "__main__":
    main()